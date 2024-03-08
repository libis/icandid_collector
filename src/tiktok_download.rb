#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'tiktok'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider}_*.rb") ].each {|file| require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

def process_recent_queries(icandid_config)
    begin

        icandid_config.queries_to_process.each.with_index() do |query, index|

            start_processing = Date.today
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_config.config[:query] = query    
            
            if query[:recent_records].nil?
                next;
            end

            if query[:recent_records][:last_run_update].nil?
                if query[:backlog][:end_date].nil?
                    start_date = Date.new(Date.today.year)
                else
                    start_date = Date.parse(query[:backlog][:end_date])
                end
            else
                start_date =  Date.parse(query[:recent_records][:last_run_update])
            end

            options = { 
                collection_type: "recent",
                start_date: start_date,
                end_date: start_processing
            }

            prepare_query(query: query, options: options, icandid_config: icandid_config)
            query[:recent_records][:last_run_update] = start_processing.strftime()
            icandid_config.update_query_config

        end
    end
end

def process_backlog_queries(icandid_config)
    begin

        icandid_config.queries_to_process.each.with_index() do |query, index|

            icandid_config.config[:query] = query    
           
            if query[:backlog].nil? || query[:backlog][:completed]
                next;
            end
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")

            start_date = Date.parse(query[:backlog][:start_date])
            end_date = Date.parse(query[:backlog][:end_date]) 

            unless query[:backlog][:current_process_date].nil? || query[:backlog][:current_process_date].empty?
                end_date = Date.parse(query[:backlog][:current_process_date]) 
            else
                query[:backlog][:current_process_date] = query[:backlog][:end_date]
            end

            @logger.debug ("query[:backlog][:current_process_date] #{query[:backlog][:current_process_date] }")

            options = { 
                collection_type: "backlog",
                start_date: start_date,
                end_date: end_date
            }

            prepare_query(query: query, options: options, icandid_config: icandid_config)

        end
    end
end

def prepare_query(icandid_config: nil, query: nil, options: {})
    begin

        start_date = options[:start_date]
        end_date = options[:end_date]

        # start_date : The lower bound of video creation time in UTC ( "20210102" )
        # end_date   : The upper bound of video creation time in UTC ( "20210123" )
        #              The end_date must be no more than 30 days after the start_date 

        current_end_date = end_date
        current_start_date = [(current_end_date - (30).days), start_date].max

        counter = 0
        while (start_date <= current_start_date  && counter < 1000) 

            @logger.debug ("get records between #{current_start_date} - #{current_end_date}")

            counter = counter + 1

            query[:query][:value]["start_date"] = (current_start_date).strftime("%Y%m%d") 
            query[:query][:value]["end_date"] = (current_end_date).strftime("%Y%m%d") 
            
            process_query(query: query, options: options, icandid_config: icandid_config)

            if options[:collection_type] == "backlog"
                query[:backlog][:current_process_date] = current_end_date.strftime("%Y%m%d")
                icandid_config.update_query_config
            end

            current_end_date = current_start_date
            current_start_date = current_end_date - (30).days

            unless current_start_date > start_date
                current_start_date = start_date
            end
            
            if current_start_date == current_end_date
                if options[:collection_type] == "backlog"
                    query[:backlog][:current_process_date] = current_end_date.strftime("%Y%m%d")
                    query[:backlog][:completed] = true
                    icandid_config.update_query_config
                end
                break
            end
        end

        query[:query][:value].delete("start_date")
        query[:query][:value].delete("end_date")

    end
end

    
def process_query(icandid_config: nil, query: nil, options: {})
    begin

        if icandid_config.config[ :rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = icandid_config.config[ :rule_set].constantize 
        end

        icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
        icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

        options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }",

        icandid_config.update_config_with_query_data( query: query, options: options )

        url = icandid_config.config[:video_url]
                
        @logger.info ("Start Download #{options[:collection_type]} query: #{ query[:query][:name] } ")
        @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")

        while (url)

            input_options = {
                bearer_token: icandid_config.config[:auth][:bearer_token],
                method: icandid_config.config[:method],
                body:   JSON.generate( query[:query][:value] )
            }
            
            icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
            data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
            
            output = DataCollector::Output.new

            rules_ng.run( rule_set[:rs_filename], data, output, options )
            rules_ng.run( rule_set[:rs_next_value], data, output, options )
            
            unless output["filename"].nil?
                filename = output["filename"].first
                file =  File.join( icandid_config.config[:source_records_dir], filename )

                icandid_output = IcandidCollector::Output.new( data: data, icandid_config: icandid_config)
                icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})
            end

            if  output["has_more"].first
                query[:query][:value]["search_id"] = output["search_id"].first
                query[:query][:value]["cursor"] = output["cursor"].first

                icandid_config.update_query_config

                output.clear

                icandid_config.update_config_with_query_data( query: query, options: options )
                url = icandid_config.config[:video_url]
            else
                query[:query][:value].delete("search_id")
                query[:query][:value].delete("cursor")
                # pp "HAS MOrE ?"
                # pp output["has_more"]
                url = nil
            end
            
        end
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
    
    icandid_config.queries_to_process.map!.with_index() do |query, index|
        query[:query][:value] = query[:query][:value].is_a?(String) ? JSON.parse(  query[:query][:value]  ) : query[:query][:value]
        query[:query][:value]["max_count"] = icandid_config.config[:records_per_page]
        query[:query][:value]["cursor"] = 0
        query[:query][:value]["search_id"] = ""
        query
    end

    process_recent_queries(icandid_config)
    process_backlog_queries(icandid_config)

end
