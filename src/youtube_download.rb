#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'pp'
require 'icandid_collector'
provider = 'YouTube'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))

pp File.join( ROOT_PATH,"src/rules/#{provider}_*.rb")

Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }


INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG
@total_nr_parsed_records = 0    

config = {
    :config_path => File.join(ROOT_PATH, "./config/#{provider}")
}

icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 

@logger.info ("Start downloading using config: #{ File.join( config[:config_path] , "config.yml") }")
start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
@logger.info ("Download for queries in : #{File.join( icandid_config.query_config.path , icandid_config.query_config.name) }")

icandid_config.queries_to_process.each.with_index() do |query, index|
  start_processing = Time.now

  @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
  icandid_config.config[:query] = query    

  
  if icandid_config.config[:rule_set].nil?
      raise "rule_set is required to parse file"
  else
      rule_set = icandid_config.config[:rule_set].constantize 
  end

  icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
  icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

  
  @logger.info ("Start Download query: #{ query[:query][:name] } ")

  
  input_options = {
      :api_key      => icandid_config.config[:auth][:api_key],
      :maxResults => 50,
      :type => "video",
      :order => "date",
      :s_query => query[:query][:value]
  }

  input_options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"

  if query[:recent_records].nil?
    # No records for this query
    @logger.info ("recent_records not configured for this query")
    next
  else 
    input_options[:publishedAfter] = query[:recent_records][:last_run_update]
  end

  icandid_config.update_config_with_query_data( query: query, options: input_options )

  @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")
  
  url = icandid_config.config[:search_url]
  

  while(url) 
    icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
    data = icandid_input.collect_data_from_uri(url: url,  options: input_options )

    if data.nil?
        @logger.warn "NO DATA AVAILABLE on this url #{url}"
        break
    end

  
    data["items"].each{ |d|

      input_options[:videoId] = d["id"]["videoId"]
      input_options[:part] = "contentDetails,id,snippet,statistics"
      icandid_config.update_config_with_query_data( query: query, options: input_options )
      url = icandid_config.config[:video_url]
      video = icandid_input.collect_data_from_uri(url: url,  options: input_options )

      output = DataCollector::Output.new
      rules_ng.run( rule_set[:rs_filename], video, output, input_options )
      rules_ng.run( rule_set[:rs_filename_audio], video, output, input_options )
      rules_ng.run( rule_set[:rs_id], video, output, input_options )
      rules_ng.run( rule_set[:rs_channelId], video, output, input_options )
      rules_ng.run( rule_set[:rs_categoryId], video, output, input_options )
      
      input_options[:channelId] = output["channelId"].first
      input_options[:categoryId] = output["categoryId"].first
      input_options[:part] = "id,snippet,statistics,status"
      
      icandid_config.update_config_with_query_data( query: query, options: input_options )
      url = icandid_config.config[:channel_url]
      video["items"][0]["channel"] = icandid_input.collect_data_from_uri(url: url,  options: input_options )["items"]

      input_options[:part] = "id,snippet"
      icandid_config.update_config_with_query_data( query: query, options: input_options )
      url = icandid_config.config[:category_url]
      video["items"][0]["category"] = icandid_input.collect_data_from_uri(url: url,  options: input_options )["items"]

      input_options[:part] = "id,replies,snippet"
      input_options[:textFormat] = "plainText"
      icandid_config.update_config_with_query_data( query: query, options: input_options )

      url = icandid_config.config[:comments_url]
      video["items"][0]["comments"] = []

      
      begin
        while (url)
          com_data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
          if com_data["items"].any?
            com_data["items"].each { |c|
              video["items"][0]["comments"].append(c)
            }
          end

          if (com_data["nextPageToken"])
            icandid_config.update_config_with_query_data( query: query, options: input_options )
            url = icandid_config.config[:comments_url] + "&pageToken=" + com_data["nextPageToken"]
          else
            url = nil
          end
        end
      rescue RuntimeError => exception
        @logger.error("Error : #{ exception } ")
      end

      unless output["filename"].nil?
        filename = output["filename"].first
        file =  File.join( icandid_config.config[:source_records_dir], filename )
        icandid_output = IcandidCollector::Output.new( data: {data: video}, icandid_config: icandid_config)
        icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})
      end
    }

    if (data["nextPageToken"])
      icandid_config.update_config_with_query_data( query: query, options: input_options )
      url = icandid_config.config[:search_url] + "&pageToken=" + data["nextPageToken"]
    else 
      url = nil
    end

  end

  query[:recent_records][:last_run_update] = start_processing.strftime("%FT%TZ")
  icandid_config.update_query_config
  
end
