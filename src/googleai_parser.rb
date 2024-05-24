#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"

require 'icandid_collector'
provider = 'GoogleAI'

PROCESS_TYPE = "parser"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0


    Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }
 
    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }

    icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    icandid_utils  = IcandidCollector::Utils.new( :icandid_config => config) 

    #pp icandid_config

    # collector = IcandidCollector::Input.new( icandid_config.config ) 
    
    @logger.info ("Start parsing using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Parsing for queries in : #{File.join( icandid_config.query_config.path , "config.yml") }")
    
    # rule_set =  icandid_config.config[:rule_set].constantize unless  icandid_config.config[:rule_set].nil?

    # pp icandid_config.config
    # pp icandid_config.queries_to_process   

    icandid_config.queries_to_process.each.with_index() do |query, index|

        @logger.info ("Paring records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")

        icandid_config.config[:query] = query

        icandid_config.config[:rule_set] = icandid_config.config[:query][:query][:rule_set]
            
        icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
        icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

        # recent_search records are downloaded to {{query_name}}/{{date}}/" 
        # - query_name is transliterated from query[:query][:name]
        # - date is download day (today) %Y_%m/%d
        options = {
            :KYE => "**",
            :date => "**",
            :id => '{{id_from_file}}'
        }

        icandid_config.update_config_with_query_data( query: query, options: options )
    
        @logger.info ("Start parsing query: #{ query[:query][:name] } ")
        @logger.info ("Start parsing source_records_dir: #{ icandid_config.config[:source_records_dir] } ")
        @logger.info ("Start parsing source_file_name_pattern: #{ icandid_config.config[:source_file_name_pattern] }")
        @logger.info ("Start parsing last_parsing_datetime: #{ icandid_config.config[:last_parsing_datetime] }")

        options = {
            :prefixid => "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }",
            :type => "CreativeWork"
        }

        icandid_input  = IcandidCollector::Input.new( :icandid_config => icandid_config)
        icandid_input.process_files( options: options  )

        @total_nr_parsed_records = @total_nr_parsed_records + icandid_input.total_nr_parsed_files
        
        query = icandid_config.config[:query]

    end
    
    icandid_config.update_query_config
    
   
rescue StandardError => e
    @logger.error("#{ e.message  }")
    @logger.error("#{ e.backtrace.inspect   }")
  
    importance = "High"
    subject = "[ERROR] iCANDID #{icandid_config.ingest_data[:provider][:name]} parsing"
    message = <<END_OF_MESSAGE
    
    <h2>Error while parsing #{icandid_config.ingest_data[:provider][:name]} data</h2>
    <p>#{e.message}</p>
    <p>#{e.backtrace.inspect}</p>
    
    <hr>
    
END_OF_MESSAGE
  
    icandid_utils.mailErrorReport(subject, message, importance, icandid_config) 
    @logger.info("#{icandid_config.ingest_data[:provider][:name]} Parsing is finished with errors")

ensure
  
    importance = "Normal"
    subject = "iCANDID #{icandid_config.ingest_data[:provider][:name]} parsing [#{@total_nr_parsed_records}]"
    message = <<END_OF_MESSAGE
    
    <h2>Parsing #{icandid_config.ingest_data[:provider][:name]} [#{icandid_config.ingest_data[:provider][:@id]}] data</h2>
    Parsing using config: : #{File.join( icandid_config.query_config.path , "config.yml") }"
  <H3>#{$0} </h3>
  command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
  
    <hr>
  
END_OF_MESSAGE

    icandid_utils.mailErrorReport(subject, message, importance, icandid_config)
    # @logger.info("#{icandid_config.ingest_data[:provider][:name]} Parsing is finished without errors")
  
end

