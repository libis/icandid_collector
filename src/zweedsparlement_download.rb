#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'ZweedsParlement'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

def handle_document(documents: nil, source_records_dir: nil)
  begin
    unless File.directory?(source_records_dir)
        FileUtils.mkdir_p(source_records_dir)
    end

    documents.each do |doc| 
      pdf_file = "#{source_records_dir}#{doc["namn"]}"

      input_options = {
        number_of_retries: 5,
        headers: {"Content-Type" => "application/pdf", "accept-encoding" => "UTF-8", "Accept" => "application/pdf"},
        verify_ssl:  false
      }
      file = @icandid_input.download_file_from_uri(url: doc["url"],  download_path: pdf_file, options: input_options )

      if file[:content_type] != "application/pdf"
        @logger.warn ("Downloaded file from #{ document_to_check } is of type [ #{ file[:content_type] } ]")
        @logger.warn ("File is saved as #{file_to_check}")
        @logger.warn ("Full text extraction for this document format available ????")
      end
    end 
    
  rescue Exception => e
      @logger.error ("Error in handle_document: #{e.message}")
  end
end


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
                end_date: "",
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
                start_date: query[:backlog][:start_date],
                end_date: query[:backlog][:end_date],
                format: "%Y-%m-%d",
                collection_type: "backlog",
                download_url_prop: "download_url",
                page: 1
            }
 
            icandid_config.prepare_query(query: query, options: options)
            url = icandid_config.config[ options[:download_url_prop].to_sym ]

            unless url.nil?

                @logger.info("download_url : #{url}")
                
                process_query(icandid_config: icandid_config, query: query, options: options)
                
                url = icandid_config.config[ options[:download_url_prop].to_sym ]
                icandid_config.update_config_with_query_data( query: query, options: options )
                icandid_config.update_query_config
    
            end

            query[:backlog][:last_run_update] = start_processing.strftime()
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
        rule_set = icandid_config.config[:rule_set].constantize 
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
            number_of_retries: 5,
            verify_ssl:  false
        }

        @icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
        data = @icandid_input.collect_data_from_uri(url: url,  options: input_options )

        if data.nil?
            @logger.warn "NO DATA AVAILABLE on this url #{url}"
            url = nil
            break
        end

        if data['dokumentlista']['dokument'].nil?
            logger.warn "NO RESULTS on this url #{url}"
            url = nil
            break
        end

        documents = data['dokumentlista']['dokument'].map{ |res| 
          unless res["filbilaga"].nil?
            res["filbilaga"]['fil'].select { |fil| fil["typ"] == "pdf" } 
          end
        }.flatten.compact
        
        handle_document(documents: documents, source_records_dir: icandid_config.config[:source_records_dir])

        output = DataCollector::Output.new
        rules_ng.run( rule_set[:rs_filename], data, output, options )
        rules_ng.run( rule_set[:rs_next_value], data, output, options )

        url = nil
        url = output[:nasta_sida]&.first
    
        unless query [ options[:collection_type].to_sym ].nil?
            query[ options[:collection_type].to_sym  ][:current_process_url] = url
        end
        icandid_config.update_query_config

        @logger.info ("NEXT URL : #{url}")

        unless output["filename"].nil?
            filename = output["filename"].first
            file =  File.join( icandid_config.config[:source_records_dir], filename )

            icandid_output = IcandidCollector::Output.new( data: {data: data}, icandid_config: icandid_config)
            icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})
        end

        if url.nil?
          query[:recent_records][:last_run_update] = query[:recent_records][:current_process_periode][:current_end_date]
          query[:recent_records][:current_process_url] = nil
          query[:recent_records][:current_process_periode] = nil
        end
        
        icandid_config.config[:query] = query  
        icandid_config.update_query_config

    end
end

begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }
    
    @logger.info ("Start downloading using config: #{ File.join( config[:config_path] , "config.yml") }")

    icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    icandid_utils  = IcandidCollector::Utils.new( :icandid_config => icandid_config.config )
        
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Download for queries in : #{File.join( icandid_config.query_config.path , icandid_config.query_config.name) }")

    process_recent_queries(icandid_config)
    process_backlog_queries(icandid_config)    

rescue => exception
    @logger.error("Error : #{ exception } ")

    importance = "High"
    subject = "iCANDID #{icandid_config.ingest_data[:provider][:name]} download [Error]"
    message = <<END_OF_MESSAGE
    
    <h2>Download #{icandid_config.ingest_data[:provider][:name]} [#{icandid_config.ingest_data[:provider][:@id]}] data</h2>
    Download using config: : #{File.join( icandid_config.query_config.path , "config.yml") }"
  <H3>#{$0} </h3>
  command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
  
  <p>Error</p>
  <br/>
  #{exception}
    <hr>
  
END_OF_MESSAGE

    icandid_utils.mailErrorReport(subject, message, importance, icandid_config)
   
ensure

    importance = "Normal"
    subject = "iCANDID #{icandid_config.ingest_data[:provider][:name]} download"
    message = <<END_OF_MESSAGE
    
    <h2>Download #{icandid_config.ingest_data[:provider][:name]} [#{icandid_config.ingest_data[:provider][:@id]}] data</h2>
    Download using config: : #{File.join( icandid_config.query_config.path , "config.yml") }"
  <H3>#{$0} </h3>
  command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
  
    <hr>
  
END_OF_MESSAGE

    begin
        icandid_utils.mailErrorReport(subject, message, importance, @icandid_config)
        # @logger.info("#{icandid_config.ingest_data[:provider][:name]} Parsing is finished without errors")
    rescue Net::SMTPFatalError => e        
        pp "Error in SMTP request"
        pp e
        pp "COULD NOT SEND EMAIL"
    rescue StandardError => e
        raise e       
    end

end

