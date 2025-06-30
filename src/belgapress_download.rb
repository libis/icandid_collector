#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'BelgaPress'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

def process_recent_queries(icandid_config)
    begin

        icandid_config.queries_to_process.each.with_index() do |query, index|

            start_processing = Date.today
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_config.config[:query] = query    
            
            if query[:recent_records].nil?
                next;
            end

            options = { 
                format: "%Y-%m-%d",
                collection_type: "recent_records",
                download_url_prop: "download_url",
                page: 1
            }

            icandid_config.prepare_query(query: query, options: options)

            url = icandid_config.config[ options[:download_url_prop].to_sym ]
            unless url.nil?

                @logger.info("download_url : #{url}")
                
                process_query(icandid_config: icandid_config, query: query, options: options)
                
                options[:page] = 1

                url = icandid_config.config[ options[:download_url_prop].to_sym ]
                icandid_config.update_config_with_query_data( query: query, options: options )
                icandid_config.update_query_config
    
            end

            query[:recent_records][:last_run_update] = start_processing.strftime()
            query[:recent_records][:current_process_url] = nil
            query[:recent_records][:current_process_periode] = nil
            
            icandid_config.config[:query] = query  
            icandid_config.update_query_config

        end
    end
end

def process_backlog_queries(icandid_config)
    begin

        icandid_config.queries_to_process.each.with_index() do |query, index|


            start_processing = Date.today
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_config.config[:query] = query    
            
            if query[:backlog].nil? || query[:backlog][:completed]
                next;
            end

            options = { 
                format: "%Y-%m-%d",
                collection_type: "backlog",
                download_url_prop: "download_url",
                page: 1
            }

            icandid_config.prepare_query(query: query, options: options)
            url = icandid_config.config[ options[:download_url_prop].to_sym ]


            until url.nil?

                @logger.info("download_url : #{url}")
                @logger.debug("save records to : #{icandid_config.config[:source_records_dir]}")

                process_query(icandid_config: icandid_config, query: query, options: options)

                options[:page] = 1

                query[:backlog][:current_process_url] = nil
                icandid_config.update_query_config
                icandid_config.prepare_query(query: query, options: options)
                url = icandid_config.config[ options[:download_url_prop].to_sym ]

            end

            query[:backlog][:completed] = true
            query[:backlog][:current_process_url] = nil
            query[:backlog][:current_process_periode] = nil
            
            icandid_config.config[:query] = query  

            icandid_config.update_query_config  

        end
    end
end

def process_query(icandid_config: nil, query: nil, options: {})

        if icandid_config.config[ :rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = icandid_config.config[ :rule_set].constantize 
        end
       
        #icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
        #icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()
        #options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"
        #icandid_config.update_config_with_query_data( query: query, options: options )

        url = icandid_config.config[ options[:download_url_prop].to_sym ]
                
        @logger.info ("Start Download #{options[:collection_type]} query: #{ query[:query][:name] } ")
        @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")

        while (url)

            input_options = {
                bearer_token: icandid_config.config[:auth][:bearer_token],
                method: icandid_config.config[:method],
                headers: icandid_config.config[:login][:headers]
            }
            
            icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
            data = icandid_input.collect_data_from_uri(url: url,  options: input_options )

            if data.nil?
                @logger.warn "NO DATA AVAILABLE on this url #{url}"
                break
            end
        
            unless (data["data"].empty? && data["_meta"]["total"] == 0)
                @logger.debug ("total record for this query : #{ data["_meta"]}")
                # Expand resultsdata to records with body
                data["data"].map!{ |d|
                    options[:uuid] = d["uuid"]
                    icandid_config.update_config_with_query_data( query: query, options: options)
                    record_url = icandid_config.config[:record_url]
                    record_data = icandid_input.collect_data_from_uri(url: record_url,  options: input_options )

                    unless record_data.nil? || record_data.empty?
                        record_data
                    else
                        d
                    end
                }

            end
            output = DataCollector::Output.new

            rules_ng.run( rule_set[:rs_filename], data, output, options )
            rules_ng.run( rule_set[:rs_next_value], data, output, options )
          
            unless output["filename"].nil?
                filename = output["filename"].first
                file =  File.join( icandid_config.config[:source_records_dir], filename )

                icandid_output = IcandidCollector::Output.new( data: data, icandid_config: icandid_config)
                icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})
            end

            unless output["next_url"].nil?
                url = output["next_url"].first 
                unless query [ options[:collection_type].to_sym ].nil?
                    query[ options[:collection_type].to_sym  ][:current_process_url] = url
                end
                icandid_config.update_query_config               
            else
                url = nil
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

    #process_recent_queries(icandid_config)
    process_backlog_queries(icandid_config)   

rescue => exception
    @logger.error("Error : #{ exception } ")
ensure
    puts "Todo : send mail ?"
end






