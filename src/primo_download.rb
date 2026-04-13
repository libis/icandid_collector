#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# Primo
# Use the primo resolver/search API
# Example: https://lib.is/search?query=any:missiezuster&from&step=10&from=0&set=primo&from_cache=0

# ===> https://developers.exlibrisgroup.com/console/?url=/wp-content/uploads/primo/openapi/primoSearch.json#//get%2Fprimo%2Fv1%2Fsearch
# offset
# The offset of the results from which to start displaying the results.
# The following example skips the first bulk and displays the remaining results up to the specified limit: offset=10.
# The recomended maximim offset is 2000.

require 'icandid_collector'
provider = 'Primo'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

def process_queries(icandid_config)
    begin
        icandid_config.queries_to_process.each.with_index() do |query, index|

            start_processing = Date.today
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_config.config[:query] = query    
           
            query[:query][:url_facet] = nil
            unless query[:query][:facet].nil?
                pp  query[:query][:facet]
                query[:query][:url_facet] = query[:query][:facet].map{ |f| "facet[]=#{CGI.escapeURIComponent(f)}"}.join('&')
            end
  
            # pp query[:url_facets] 

            if query[:params].nil? || query[:params][:completed]
                @logger.info "query[:params] are nil or completed is true"
                next;
            end

            options = { 
                download_url_prop: "download_url"
            }

            icandid_config.prepare_query(query: query, options: options)
            url = icandid_config.config[ options[:download_url_prop].to_sym ]

            unless url.nil?

                @logger.info("download_url : #{url}")
                process_query(icandid_config: icandid_config, query: query, options: options)
    
            end
            query[:query][:url_facet] = nil
            query[:params][:last_run_update] = start_processing.strftime()
            icandid_config.update_query_config

        end
    end
end

def filter_error(errors)
    errors = [errors] if ! errors.is_a?(Array)

    #puts "============================================================="
    #pp errors
    #puts "============================================================="
    # =begin    
    #     errors.select! { |e|
    #     !(e["title"] == "Forbidden" && e["resource_type"] == "user") &&
    #     !(e["title"] == "Not Found Error" && e["resource_type"] == "user") &&
    #     !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "referenced_tweets.id") &&
    #     !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "edit_history_tweet_ids") &&      
    #     !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "attachments.media_source_tweet") &&      
    #     !(e["title"] == "Authorization Error" && e["resource_type"] == "tweet")
    #     }
    # =end
    errors
  end
  


def process_query(icandid_config: nil, query: nil, options: {})

    if icandid_config.config[:rule_set].nil?
        raise "rule_set is required to parse file"
    else
        rule_set = icandid_config.config[:rule_set].constantize 
    end

    icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
    icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

    options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"


    if query[:params][:from].nil?
        query[:params][:from] =  0
        query[:params][:step] =  10
        icandid_config.update_query_config
        query = icandid_config.queries_to_process.select{ |ptop| ptop[:internal_collector_id] == query[:internal_collector_id] }.first
        icandid_config.config[:query] = query
    end


    icandid_config.update_config_with_query_data( query: query, options: options )

    url = icandid_config.config[ options[:download_url_prop].to_sym ]

    @logger.info ("Start Download query: #{ query[:query][:name] } ")
    @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")
    
    while (url)
        
        @logger.info ("Start Download from url: #{ url } ")


        input_options = {
            # bearer_token: icandid_config.config[:auth][:bearer_token],
            method: icandid_config.config[:method],
            number_of_retries: 5
        }
        
        # url = "file:///app/src/reddit_test.json"
        icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
        data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
       
        if data.nil?
            @logger.warn "NO DATA AVAILABLE on this url #{url}"
            break
        end
        output = DataCollector::Output.new

        options[:download_url] = url

        rules_ng.run( rule_set[:rs_filename], data, output, options )
        rules_ng.run( rule_set[:rs_next_value], data, output, options )
        rules_ng.run( rule_set[:rs_raw_data], data, output, options )

        #pp "raw_data"
        #pp output["data"].first["created_utc"]
        #pp "first_date: #{output["data"].first["created_utc"]} => #{ Time.at( output["data"].first["created_utc"] ) }"
        #pp "last_date:  #{output["data"].last["created_utc"] } => #{ Time.at( output["data"].last["created_utc"] ) }"
        #pp "query[:params][:start_date] : #{ Time.parse(  query[:params][:start_date] ) }"
   
        unless output["error"].nil?

            errors = filter_error( output["error"] )


            unless errors.empty?
              url=nil
              @logger.error ("error in API-response: #{ output["error"] } ")
              raise "Error in request"
            end
        end
        

        unless output["filename"].nil?
            filename = output["filename"].first

            file =  File.join( icandid_config.config[:source_records_dir], filename )
            # add metadata to the downloaded file

            output_data = {}
            output_data[:data] = output["data"]
          
            output_data[:metadata] = output["metadata"].one? ? output["metadata"].first : output["metadata"] 

            icandid_output = IcandidCollector::Output.new( data: output_data, icandid_config: icandid_config)
            icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})

        end


        unless output["next_token"].nil?

            pp "output[next_token]output[next_token] : #{output["next_token"]}"
            query[:params][:from] = output["next_token"].first["from"]
            query[:params][:step] = output["next_token"].first["step"]
            
            icandid_config.update_query_config
            query = icandid_config.queries_to_process.select{ |ptop| ptop[:internal_collector_id] == query[:internal_collector_id] }.first

            icandid_config.config[:query] = query
  
            icandid_config.update_config_with_query_data( query: query, options: options )
            url = icandid_config.config[:download_url]
            # sleep rand(20)
        else
            query[:params][:completed] = true
            query[:params][:from] = 0
            # pp check for errors
            icandid_config.update_query_config
            url = nil
        end

        if output["next_token"].first["from"] > output["next_token"].first["count"]
            query[:params][:completed] = true
            query[:params][:from] = nil
            icandid_config.update_query_config
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

    process_queries(icandid_config)

rescue => exception
    @logger.error("Error : #{ exception } ")
ensure
    puts "Todo : send mail ?"
end






