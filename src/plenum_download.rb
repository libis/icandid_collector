#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')


require 'icandid_collector'
provider = 'plenum'

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
            
            if query[:params].nil? || query[:params][:completed]
                next;
            end

            options = {  }

            prepare_query(query: query, options: options, icandid_config: icandid_config)
            
            query[:params][:last_run_update] = start_processing.strftime()
        
            icandid_config.update_query_config
        end
    end
end


def prepare_query(icandid_config: nil, query: nil, options: {})
    begin
        process_query(query: query, options: options, icandid_config: icandid_config)       
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
   
    
    url = icandid_config.config[:sessions_url]

    while (url)



        
        @logger.info ("Start Download from url: #{ url } ")

        input_options = {
            method: icandid_config.config[:method],
            number_of_retries: 5
        }
        
        icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
        data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
        
        if data.nil?
            @logger.warn "NO DATA AVAILABLE on this url #{url}"
            break
        end
   
        unless (data.empty?)
            # Expand resultsdata to records with body
            data.map!{ |d|
                unless  d["id"].nil?
                    input_options[:session_id] = d["id"]

                    icandid_config.update_config_with_query_data( query: query, options: input_options)
                    record_txt_url = icandid_config.config[:txt_url]
                    record_pdf_url = icandid_config.config[:pdf_url]

                    pp "record_txt_url record_txt_url record_txt_url record_txt_url"
                    pp record_txt_url

                    pp "record_pdf_url record_pdf_url record_pdf_url record_pdf_url"
                    pp record_pdf_url

                    unless record_txt_url.nil?
                        @logger.debug ("download #{record_txt_url} [txt] for #{d["id"]} ")
                        http = HTTP
                        http_response = http.follow.get(record_txt_url, {})
                        d['text'] = http_response.body.to_s
                    end

                    unless record_pdf_url.nil?
                        d['sameAs'] = record_pdf_url
                        # @logger.debug ("download #{record_pdf_url} for #{d["id"]} ")

                        # pdf_file = "#{ icandid_config.config[:source_records_dir]  }/#{d["id"]}.pdf"
                        # @logger.debug ("download to #{pdf_file} ")
                        # File.open(pdf_file, "wb") do |file|
                        #    file.write URI.open(record_pdf_url).read
                        # end
                    end
                end
                d
            }
        
            output = DataCollector::Output.new
            rules_ng.run( rule_set[:rs_next_value], data, output, options )
            rules_ng.run( rule_set[:rs_raw_data], data, output, options )

            unless output["error"].nil?

                errors =  output["error"]
                
                unless errors.empty?
                url=nil
                @logger.error ("error in APi-response: #{ output["error"] } ")
                raise "Error in request"
                end
            end
            
            filename = "#{data.first["date"].to_date.strftime('%Y%m%d') }_#{data.first["id"]}_#{data.last["id"]}.json"
            file =  File.join( icandid_config.config[:source_records_dir], filename )
            icandid_output = IcandidCollector::Output.new( data: {data: data}, icandid_config: icandid_config)
            icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})

            unless output["next_token"].nil?
                query[:params][:next_token] = output["next_token"].first 
                icandid_config.update_query_config
                query = icandid_config.queries_to_process.select{ |ptop| ptop[:internal_collector_id] == query[:internal_collector_id] }.first

                icandid_config.config[:query] = query
                icandid_config.update_config_with_query_data( query: query, options: options )
                url = icandid_config.config[:sessions_url]
            end
        end


        if data.empty? || query[:params][:end_date].to_date < data.last["date"].to_date
            query[:params][:completed] = true
            query[:params][:next_token] = nil
            # pp check for errors
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






