#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'TMDB'

PROCESS_TYPE = "parser"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

def parse_recent_queries( options: {})
    options = { 
        date: "????",
        collection_type: "recent"
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

        options[:type] = "Movie"

        @icandid_config.queries_to_process.each do |query|
            @icandid_config.config[:query] = query
            @icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
            @icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

            # options[:date] = ""

            @icandid_config.update_config_with_query_data( query: query, options: options )    

            @logger.info ("Parse records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            
            icandid_input  = IcandidCollector::Input.new( :icandid_config => @icandid_config)
         
            @logger.info ("Start parsing query: #{ query[:query][:name] } ")
            @logger.info ("Start parsing source_records_dir: #{@icandid_config.config[:source_records_dir]} ")
            @logger.info ("Start parsing source_file_name_pattern: #{@icandid_config.config[:source_file_name_pattern]} ")

            icandid_input.process_files( options: options  )
            @logger.info ("Start parsing next NEXT NEXT ")

        end
    end
end

begin

    start_processing =  Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::ERROR
    @total_nr_parsed_records = 0    
    @icandid_utils  = IcandidCollector::Utils.new()

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }

    @icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    
    @logger.info ("Start downloading using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Download for queries in : #{File.join( @icandid_config.query_config.path , @icandid_config.query_config.name) }")
    
    @icandid_config.queries_to_process.map! do |query|
           query
    end

    options = {
        prefixid: "#{@icandid_config.ingest_data[:prefixid]}_#{ @icandid_config.ingest_data[:provider][:@id].downcase }_#{ @icandid_config.ingest_data[:dataset][:@id].downcase }",
        ingest_data: @icandid_config.ingest_data
    }

    parse_recent_queries(options: options)
#    parse_backlog_queries(options: options)
    
    @icandid_config.queries_to_process.map! do |query|
        query[:last_parsing_datetime] = start_processing

        query
    end
    @icandid_config.update_query_config

end
