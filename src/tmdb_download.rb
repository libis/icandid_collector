#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'TMDB'


PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)


#############################################################
TESTING = false
STATUS = "downloading"

COUNT = 100 

#START_YEAR = 1945
#END_YEAR = 2024


def process_queries(icandid_config)
  begin
    icandid_config.queries_to_process.each.with_index() do |query, index|
      if query[:params].nil? || query[:completed]
        next;
      end

      start_processing = Date.today
      @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
      icandid_config.config[:query] = query    


      for year in query[:params][:start_year]..query[:params][:end_year] do    
        daysinmonth = [0,31,28,31,30,31,30,31,31,30,31,30,31]
        if year % 4 
          daysinmonth[2] = 29
        end

        for month in 1..12 do
          for d_from in [1,9,17,25] do
            d_to = d_from + 7
            if d_to > daysinmonth[month]
              d_to = daysinmonth[month]
            end

            options = { 
                download_url_prop: "discover_url",
                primary_release_date_gte: "%04d-%02d-%02d" % [year,month,d_from],
                primary_release_date_lte: "%04d-%02d-%02d" % [year,month,d_to]
            }
          
            icandid_config.prepare_query(query: query, options: options)
            url = icandid_config.config[ options[:download_url_prop].to_sym ]
            
            unless url.nil?
                @logger.info("download_url : #{url}")
                process_query(icandid_config: icandid_config, query: query, options: options)
            end
          end
        end
      end
      exit
      query[:params][:last_run_update] = start_processing.strftime()
      icandid_config.update_query_config
    end
  end
end



def process_query(icandid_config: nil, query: nil, options: {})

    if icandid_config.config[:rule_set].nil?
        raise "rule_set is required to parse file"
    else
        rule_set = icandid_config.config[ :rule_set].constantize 
    end

    icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
    icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

    options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"

    icandid_config.update_config_with_query_data( query: query, options: options )

    @logger.info ("Start Download query: #{ query[:query][:name] } ")
    @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")
       
    url = icandid_config.config[ options[:download_url_prop].to_sym ]
    while (url)
        
      @logger.info ("Start Download from url: #{ url } ")

      input_options = {
          method: icandid_config.config[:method],
          number_of_retries: 5
      }
      
      icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
      data = icandid_input.collect_data_from_uri(url: url,  options: input_options )


      pagecount = data["total_pages"]
      currentpage = 1

      while currentpage <= pagecount
        unless (data["results"].empty?)
            @logger.debug ("total record for this query : #{ data["total_results"]}")
            # Expand resultsdata to records with body
            data["results"].each{ |d|
                options[:movie_id] = d["id"]
                
                icandid_config.update_config_with_query_data( query: query, options: options )
                record_url = icandid_config.config[:record_url]

                icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
                @logger.info("Details from this url : #{record_url}")
                record_data = icandid_input.collect_data_from_uri(url: record_url,  options: input_options )

                unless record_data.nil? || record_data.empty?
                    record_data = record_data.compact
                    filename = "#{d["id"]}"
                    @logger.info("Writing to #{filename}")
                    file =  File.join( icandid_config.config[:source_records_dir], filename )
                    icandid_output = IcandidCollector::Output.new( data: {data: record_data}, icandid_config: icandid_config)
                    icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})         
                    sleep(1)
                    exit if TESTING
                end
            }   
        end

        currentpage = currentpage + 1
        options[:page] = currentpage

        icandid_config.update_config_with_query_data( query: query, options: options )
        
      end 
      url = nil
    end
   
end

begin

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

  process_queries(icandid_config)


rescue => exception
    @logger.error("Error : #{ exception } ")
ensure
    puts "Todo : send mail ?"
end
