#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'ZweedsParlement'

PROCESS_TYPE = "parser"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

def parse_recent_queries( options: {})
    options = { 
        date: "*/**",
        collection_type: "recent_records"
    }
    parse_queries(options: options)
end

def parse_backlog_queries( options: {})
    options = { 
        date: "*/backlog",
        collection_type: "backlog"
    }
    parse_queries(options: options)
end

def parse_queries(options: {})
    begin
        if @icandid_config.config[ :rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = @icandid_config.config[:rule_set].constantize 
        end

        input_options = {
          number_of_retries: 3,
          headers: {"Content-Type" => "application/json", "accept-encoding" => "UTF-8", "Accept" => "application/json"},
          verify_ssl:  false
        }
        begin
           doktyp   =  @icandid_input.collect_data_from_uri(url: "https://data.riksdagen.se/sv/koder/?typ=doktyp&utformat=json" , options: input_options )
           organ    =  @icandid_input.collect_data_from_uri(url: "https://data.riksdagen.se/sv/koder/?typ=organ&utformat=json" , options: input_options )
           roll     =  @icandid_input.collect_data_from_uri(url: "https://data.riksdagen.se/sv/koder/?typ=roll&utformat=json" , options: input_options )
           riksmote =  @icandid_input.collect_data_from_uri(url: "https://data.riksdagen.se/sv/koder/?typ=riksmote&utformat=json" , options: input_options )
        rescue DataCollector::InputError => e                        
          @logger.error (e)
          exit
        end
        
        
        options[:type] = "Legislation"
        options[:default_publisher] = {
                :@type => "Organization",
                :@id   => "iCANDID_ORGANIZATION_ZWEEDS_PARLEMENT",
                :name  => "Sveriges riksdag"
            }
        options[:doktyp] = doktyp["typer"]["typ"]
        options[:organ] = organ["organ"]["organ"]
        options[:roll] = roll
        options[:riksmote] = riksmote
        

        @icandid_config.queries_to_process.each do |query|
            @icandid_config.config[:query] = query
            @icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
            @icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

            @icandid_config.update_config_with_query_data( query: query, options: options )    

            @logger.info ("Parse records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_input  = IcandidCollector::Input.new( :icandid_config => @icandid_config)#
           
            @logger.info ("Start parsing query: #{ query[:query][:name] } ")
            @logger.info ("Start parsing source_records_dir: #{@icandid_config.config[:source_records_dir]} ")
            @logger.info ("Start parsing source_file_name_pattern: #{@icandid_config.config[:source_file_name_pattern]} ")

            
            icandid_input.process_files( options: options  )



            @total_nr_parsed_files = @total_nr_parsed_files + icandid_input.total_nr_parsed_files
            @logger.info ("#{@total_nr_parsed_files} files parsed")
            @logger.info ("#{@icandid_config.config[:nbr_created_records]} records created")
            @logger.info ("Start parsing next NEXT NEXT ")

        end
    end
end


begin

    start_processing =  Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_files = 0    
    
    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }

    @icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA)
    @icandid_input  = IcandidCollector::Input.new( :icandid_config => @icandid_config.config ) 
    @icandid_utils  = IcandidCollector::Utils.new( :icandid_config => @icandid_config.config )

    
    @logger.info ("Start parsing using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Parsing for queries in : #{File.join( @icandid_config.query_config.path , @icandid_config.query_config.name) }")
    

    options = {
        prefixid: "#{@icandid_config.ingest_data[:prefixid]}_#{ @icandid_config.ingest_data[:provider][:@id].downcase }_#{ @icandid_config.ingest_data[:dataset][:@id].downcase }",
        ingest_data: @icandid_config.ingest_data
    }

    # split to avoid looking for new files in the backlog directories
    parse_recent_queries(options: options)
#    parse_backlog_queries(options: options)
    
    @icandid_config.queries_to_process.map! do |query|
        query[:last_parsing_datetime] = start_processing
        query
    end
    @icandid_config.update_query_config

  
rescue StandardError => e
    @logger.error("#{ e.message  }")
    @logger.error("#{ e.backtrace.inspect   }")
  
    importance = "High"
    subject = "[ERROR] iCANDID #{@icandid_config.ingest_data[:provider][:name]} parsing"
    message = <<END_OF_MESSAGE
    
    <h2>Error while parsing #{@icandid_config.ingest_data[:provider][:name]} data</h2>
    <p>#{e.message}</p>
    <p>#{e.backtrace.inspect}</p>
    
    <hr>
    
END_OF_MESSAGE
  
    @icandid_utils.mailErrorReport(subject, message, importance, @icandid_config) 
    @logger.info("#{@icandid_config.ingest_data[:provider][:name]} Parsing is finished with errors")

ensure
  
    importance = "Normal"
    subject = "iCANDID #{@icandid_config.ingest_data[:provider][:name]} parsing [#{@total_nr_parsed_files} => #{@icandid_config.config[:nbr_created_records]}]"
    message = <<END_OF_MESSAGE
    
    <h2>Parsing #{@icandid_config.ingest_data[:provider][:name]} [#{@icandid_config.ingest_data[:provider][:@id]}] data</h2>
    Parsing using config: : #{File.join( @icandid_config.query_config.path , "config.yml") }"
  <H3>#{$0} </h3>
  command_line_options :<br/> #{ @icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
    <br/>
    total_nr_parsed_files : #{@total_nr_parsed_files}
    <br/>
    nbr_created_records : #{@icandid_config.config[:nbr_created_records]}

  
    <hr>
  
END_OF_MESSAGE

    begin
        @icandid_utils.mailErrorReport(subject, message, importance, @icandid_config)
        # @logger.info("#{icandid_config.ingest_data[:provider][:name]} Parsing is finished without errors")
    rescue Net::SMTPFatalError => e        
        pp "Error in SMTP request"
        pp e
        pp "COULD NOT SEND EMAIL"
    rescue StandardError => e
        raise e       
    end
  
end