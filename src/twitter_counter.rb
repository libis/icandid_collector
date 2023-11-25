#encoding: UTF-8
#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
require 'csv'
provider = "twitter"


PROCESS_TYPE = "parser"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }
    
    icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    
    @logger.info ("Start count using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Count for queries in : #{File.join( icandid_config.query_config.path , "config.yml") }")

    icandid_config.queries_to_parse.each.with_index() do |query, index|
        json_count = []
        next_url = ""
        @logger.info ("Count records in backlog for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")

        icandid_config.config[:query] = query    

        query[:query][:value] = URI.encode_www_form_component( query[:query][:value] )

        query[:query][:start_time] = query[:backlog][:start_date]
        query[:query][:end_time] = query[:backlog][:end_date]

        @logger.info ("Count records for backlog  #{ query[:query][:start_time]  } [ #{query[:query][:end_time] } ]")

        icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
        icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

        options = {
            prefixid: "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"
        }

        icandid_config.update_config_with_query_data( query: query, options: options )
    
        @logger.info ("Start Count query: #{ query[:query][:name] } ")
        @logger.info ("Start Count source_records_dir: #{ icandid_config.config[:source_records_dir] } ")

        options = {
            bearer_token: icandid_config.config[:auth][:bearer_token],
            method: icandid_config.config[:method],
            next_token: nil          
        }
        icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)

        while !next_url.nil?
            data = icandid_input.collect_data_from_uri(url: icandid_config.config[:count_url],  options: options  )
            if data.nil?
                @logger.warn "NO DATA AVAILABLE on this url #{url}"
                break
            end
            if data["meta"]["result_count"] == 0
                @logger.warn "NO RESULTS AVAILABLE for this query:   #{query_url}"
                break
            end

            json_count.prepend( data["data"] )
            if data["meta"]["next_token"].nil?
                next_url= nil
            else
                options[:next_token] = "next_token=#{data["meta"]["next_token"]}"
                sleep 5
                icandid_config.update_config_with_query_data( query: query, options: options )
            end
        end

        json_count.flatten!

        csv_string = CSV.generate do |csv|
            json_count.each do |hash|
              csv << hash.values
            end
        end


        csv_dir =  File.join( '/source_records/Twitter/', query[:query][:id] )
               
        Dir.mkdir(csv_dir) unless File.exists?(csv_dir)

        csv_file = File.join( csv_dir , "count_#{query[:query][:id]}.csv")
        
        File.open(csv_file, 'w') { |file| file.write(  csv_string) }

        @logger.info ("Count results available in: #{ csv_file} ")


        exit
    end


end





        

