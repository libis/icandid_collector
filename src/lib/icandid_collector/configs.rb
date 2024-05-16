#encoding: UTF-8
require 'yaml'
require 'optparse'
require 'mustache'

module IcandidCollector 

  class Configs

    attr_accessor :init_config, :config, :query_config, :queries_to_process, :retries, :ingest_data

    def initialize( config: {}, root_path: ROOT_PATH, ingest_data: {} )
      
      @logger = Logger.new(STDOUT)
      @retries = 0
      @command_line_options = {}
      @init_config = {}

      @config_class = DataCollector::ConfigFile.clone
      @config_class.path = config[:config_path]

      @config_class.keys.each { |k|
        @init_config[k] = @config_class[k]
      }

      @query_config  = DataCollector::ConfigFile.clone
      @query_config.path = config[:query_config_path] ||  File.join(config[:config_path], 'queries')
      @queries_to_process = {}


      @icandid_data = JSON.parse( File.read(File.join(root_path, './config/config.cfg')) , :symbolize_names => true)
      unless ingest_data.empty?
        @ingest_data = ingest_data

        @ingest_data[:prefixid] = @icandid_data[:prefixid]
        @ingest_data[:url_prefix] = @icandid_data[:url_prefix]
        @ingest_data[:genericRecordDesc] = "Entry from #{ @ingest_data[:dataset][:name]}" 

      end
      update_config_with_command_line_options()
      get_queries_to_process()

      @config = @init_config.clone

    end

    def config
      @config
    end

    def icandid_data
      @icandid_data
    end

    def ingest_data
      @ingest_data
    end

    def load_config_from_file( path: './config', file: 'config.yml')
      YAML.load( File.read(File.join( path, file) ) )
    end

    def update_init_config(key_path: nil, value: nil)
      key_path = key_path.split('.').map(&:to_sym)
      last_key = key_path.pop() 
      (@init_config.dig *key_path)[last_key] = value
      
      @config_class.keys.each { |k|
        @config_class[k] = @init_config[k]
      }
    end

    def command_line_options
      @command_line_options
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
            @init_config = YAML.load( File.read( @command_line_options[:config] ) )
        else
            raise ("config #{@command_line_options[:config]} does not exist")
        end
      end

      unless @command_line_options[:query_config].nil?

          if File.exist?(@command_line_options[:query_config])
            @query_config.path = File.dirname(@command_line_options[:query_config])
            @query_config.name = File.basename(@command_line_options[:query_config])

            # pp File.dirname(@command_line_options[:query_config])
            # pp File.basename(@command_line_options[:query_config])
          else
            raise ("config #{@command_line_options[:query_config]} does not exist")
          end
      end

      unless @command_line_options[:source_dir].nil?
        @init_config[:source_records_dir] = @command_line_options[:source_dir] 
      end

      unless @command_line_options[:source_file_name_pattern].nil?
        @init_config[:source_file_name_pattern] = @command_line_options[:source_file_name_pattern] 
      end

      unless @command_line_options[:dest_dir].nil?
        @init_config[:records_dir] = @command_line_options[:dest_dir] 
      end
    end

    def get_queries_to_process( )
      queries_to_process = @query_config[:queries]
      unless @command_line_options[:query_id].nil?
        query_ids_to_parse = @command_line_options[:query_id].split(",")
        queries_to_process = @query_config[:queries].select { |q| query_ids_to_parse.include?(  q[:query][:id] ) }
        if queries_to_process.empty?
          raise ("#{@command_line_options[:query_id]} does not exist in #{ @query_config.path }/config.cfg")
        end
      end
      unless @command_line_options[:last_parsing_datetime].nil?
        queries_to_process = queries_to_process.map { |q| q[:last_parsing_datetime] = @command_line_options[:last_parsing_datetime]; q  }
      end

      @queries_to_process  = queries_to_process
    end

    def update_config_with_query_data( query:{}, options:{})
      @config = @config.map { |k, v| 
        unless @init_config[k].nil?
          v = @init_config[k].clone()
        end
        options[k] = v
        [k, v]
      }.to_h


      if options[:date].nil?
        options[:date]  = Time.now.strftime("%Y/%m/%d")  
        if options[:collection_type] == "recent_records"
          options[:date]       = Time.now.strftime("%Y_%m/%d")  
        end
        if options[:collection_type] == "backlog"
          options[:date] = "#{Time.now.strftime("%Y-%m-%d")}/backlog/#{  query[:backlog][:current_process_date].to_datetime.strftime("%Y_%m") }/"
        end
      end
      if options[:records_dir_date].nil?
        options[:records_dir_date] = Time.now.strftime("%Y/%m/%d")
      end


      options[:today] = Time.now.strftime("%Y/%m/%d")
      options[:year]  = Time.now.strftime("%Y")
      options[:month] = Time.now.strftime("%m")
      options[:day]   = Time.now.strftime("%d")
      options[:hour]  = Time.now.strftime("%H")

      @config = JSON.parse( Mustache.render(JSON.generate(@config), options),  :symbolize_names => true)
  
      #@config[:source_records_dir]    = get_source_records_dir( options: options)
      #@config[:records_dir]           = get_records_dir( options: options)
      #@config[:additional_dirs]       = get_additional_dirs( options: options)     

      @config[:last_parsing_datetime] = get_parsing_datetime( query: query )

    end

    def get_parsing_datetime( query:{} )
      # puts  query[:last_parsing_datetime] 
      # puts "get_parsing_datetime"      
      # puts  @command_line_options[:last_parsing_datetime]
      return Time.parse( @command_line_options[:last_parsing_datetime] ) unless  @command_line_options[:last_parsing_datetime].nil?
      return Time.parse( query[:last_parsing_datetime] ) unless query[:last_parsing_datetime] .nil? ||query[:last_parsing_datetime].empty?
      return Time.parse("2000/01/01")
    end
  
    def update_query_config
      if @command_line_options[:last_parsing_datetime].nil?
        new_queries = @query_config[:queries].map { |q| 
          new_q = @queries_to_process.select{ |ptop| ptop[:query][:id] == q[:query][:id] }.first
          unless new_q.nil?
            p = proc { |v1, v2| 
              result = {}
              v1.each do |k, v|
                unless v2[k].nil?
                  if v1[k].is_a?(Hash)
                    v = p.call(v1[k],v2[k])
                  else
                    v = v2[k]
                  end
                end
                result.store(k.to_sym, v)
              end
              result
            }
            q = p.call(q,new_q)
          end
          q
        }
        @query_config[:queries] = new_queries
      end
    end


  end
 
end