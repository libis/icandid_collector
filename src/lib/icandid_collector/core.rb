#encoding: UTF-8
require 'yaml'
require 'logger'
require 'data_collector'
require 'timeout'

require_relative 'utils'
require_relative 'configs'

=begin
module IcandidCollector
  extend DataCollector::Core
  
  ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
  FROM_MAIL_ADDRESS = "icandid@libis.kuleuven.be"
  SMTP_SERVER = "smtp.kuleuven.be"

  

  class Error < StandardError; end
end


module IcandidCollector 
  
  ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
  FROM_MAIL_ADDRESS = "icandid@libis.kuleuven.be"
  SMTP_SERVER = "smtp.kuleuven.be"

  extend DataCollector::Core

  class Input < DataCollector::Input

  end

  class Config 

    attr_accessor :config, :query_config, :retries, :ingest_config

    def initialize( config: {}, root_path: "./", ingest_config: {} )
      
      @logger = Logger.new(STDOUT)
      @retries = 0
      @command_line_options = {}
      @config = DataCollector::ConfigFile.clone

      @config.path = config[:config_path]
      # @config.file = config[:config_file]
      
      @query_config  = DataCollector::ConfigFile.clone
      @query_config.path = config[:query_config_path]
      # @query_config.file = config[:query_config_file]

      @icandid_conf = JSON.parse( File.read(File.join(root_path, './config/config.cfg')) , :symbolize_names => true)
      @ingest_config = ingest_config

      @ingest_config[:prefixid] = @icandid_conf[:prefixid]
      @ingest_config[:url_prefix] = @icandid_conf[:url_prefix]
      @ingest_config[:genericRecordDesc] = "Entry from #{ @ingest_config[:dataset][:name]}"

      update_config_with_command_line_options()

    end

    def get_command_line_options()

      if PROCESS_TYPE == "count"
        option_parser = OptionParser.new do |o|
          o.banner = "Usage: #{$0} [options]"
          o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
          o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
          o.on("-n QUERY_ID", "--query_id QUERY_ID", "=MANDATORY", "The id of one specific query that needs to be counted") { |n| @command_line_options[:query_id] = n}
          o.on( '-h', '--help', 'Display this screen.' ) do
            puts o
            exit
          end
          o.parse!   
        end
        if  @command_line_options[:query_id].nil?
          puts option_parser.help
          exit 1
        end
      end

      if PROCESS_TYPE == "download"
        option_parser = OptionParser.new do |o|
          o.banner = "Usage: #{$0} [options]"
          o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
          o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
          o.on("-n QUERY_ID", "--query_id", "The id of the query if only one specific query needs to be downloaded") { |n| @command_line_options[:query_id] = n}
          o.on("-s SOURCE_RECORDS_DIR", "--source", "Directory where the records will be stored (/source_records/<provider>/{{query}}/)") { |s| @command_line_options[:source_dir] = s}
          o.on( '-h', '--help', 'Display this screen.' ) do
            puts o
            exit
          end
          o.parse!   
        end
      end

      if PROCESS_TYPE == "parser"
        option_parser = OptionParser.new do |o|
            o.banner = "Usage: #{$0} [options]"
          # o.on("-l LOGFILE", "--log", "write log to file") { |log_file| @log_file = log_file}
            o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
            o.on("-i INGEST_FILE", "--ingest", "Ingest config file") { |i| ingest_file = i;  @command_line_options[:ingest_file] = i}
            o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
            o.on("-n QUERY_ID", "--query_id", "The id of the query if only one specific query needs to be parsed") { |n| @command_line_options[:query_id] = n}
            o.on("-s SOURCE_RECORDS_DIR", "--source", "Directory of the source records (/source_records/<provider>/{{query}}/") { |s| @command_line_options[:source_dir] = s}
            o.on("-d DESTINATION_RECORDS_DIR", "--destination", "Directory to save the parsend schema.org json-ld records (/records/<provider>/{{query_id}})") { |d| @command_line_options[:dest_dir] = d}
            o.on("-p FILE_PATTERN", "--pattern", "file pattern of record-filename /<provider>*\\\.json/") { |p| @command_line_options[:source_file_name_pattern] = p}
            o.on("-u LAST_RUN", "--last_parsing_datetime", "Time the command was last run. Load files with modification time > LAST_RUN ") { |u| @command_line_options[:last_parsing_datetime] = u }    
            o.on("-b BASED_ON_DATE_PUBLISHED", "--dir_based_on_datePublished", "Make subfolders in destination based on DatePublished in the record (true/false)") { |b| @command_line_options[:dir_based_on_datePublished] = b }    
            o.on( '-h', '--help', 'Display this screen.' ) do
              puts o
              exit
            end
          o.parse!   
        end 
      end

    end

    def update_config_with_command_line_options()
      @command_line_options = {}
      get_command_line_options
      unless @command_line_options[:config].nil?
        if File.exist?(@command_line_options[:config])
            @config.path = File.dirname(@command_line_options[:config])
            @config.file  = File.basename(@command_line_options[:config])
        else
            raise ("config #{@command_line_options[:config]} does not exist")
        end
      end

      unless @command_line_options[:query_config].nil?
          if File.exist?(@command_line_options[:query_config])
            @query_config.path = File.dirname(@command_line_options[:query_config])
            @query_config.file = File.basename(@command_line_options[:query_config])
          else
            raise ("config #{@command_line_options[:query_config]} does not exist")
          end
      end
    end

    def command_line_options
      @command_line_options
    end

  end

  class Utils
    
    attr_accessor :mail_to, :mail_from, :smtp_server

    def initialize( mail_to: ADMIN_MAIL_ADDRESS, mail_from: FROM_MAIL_ADDRESS, smtp_server: SMTP_SERVER)
      @to_address = mail_to
      @from_address = mail_from
      @smtp_server = smtp_server
    end

    def mailErrorReport (subject,  report , importance, config)
      now = DateTime.now
  
      message = <<END_OF_MESSAGE
From: #{ @from_address }
To: #{@to_address}
MIME-Version: 1.0
Content-type: text/html
Subject: #{subject}
importance: #{importance}
Date: #{ now }

<H1>#{subject}</H1>

#{report}

END_OF_MESSAGE


      pp  @smtp_server

      Net::SMTP.start(@smtp_server, 25) do |smtp|
          smtp.send_message message,
          @from_address , @to_address
      end
    end
  end

end
=end