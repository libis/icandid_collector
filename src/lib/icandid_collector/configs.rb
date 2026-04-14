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

      queries_to_process = @query_config[:queries].map { |q| 
        unless q.has_key?(:internal_collector_id) && ! q[:internal_collector_id].nil?
          q[:internal_collector_id] = rand(36**10).to_s(36)
        end
        q
      }
      @query_config[:queries] = queries_to_process

      internal_collector_id_array = @query_config[:queries].map { |q| q[:internal_collector_id] }
      raise 'internal_collector_id in query config file contains duplicate values!' unless internal_collector_id_array.uniq.length == internal_collector_id_array.length

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

    def get_periode(options)  

      # rake test TEST=test/data_collector_start_end_date.rb TESTOPTS="--name=test_date_last_in_backlog -v"
  
      # Method to calculate the current period (start and end dates) that is processed based on options provided.
      # 
      # options [String, Date] :start_date The lower bound of record creation time in UTC (e.g., "20210102").
      # options [String, Date] :end_date The upper bound of record creation time in UTC (e.g., "20210123").
      # options [String, nil]] :format The desired date format for the output. default = "%Y-%m-%d". Optional !
      # options [String, Date, nil] :current_start_date The start date of the current period, if provided.
      # options [String, Date, nil] :current_end_date The end date of the current period, if provided.
      # options [String, nil] :collection_type Specifies the type of collection ("backlog").
      # 
      # @return [Hash] A hash containing :current_start_date and :current_end_date formatted as Date.
  
      format = options[:format] || "%Y-%m-%d"
  
      start_date = options[:start_date]
      end_date = options[:end_date]
      current_end_date = options[:current_end_date].nil? ? end_date : options[:current_end_date] 
      current_start_date = options[:current_start_date].nil? ? start_date : options[:current_start_date]
  
      max_time_interval = options[:max_time_interval].nil? ? "1.month" : options[:max_time_interval] 
  
      (start_date, end_date, current_end_date, current_start_date) = 
        [start_date, end_date, current_end_date, current_start_date].map do |d| 
          begin
            if d.nil?
              d
            elsif d.is_a?(DateTime) || d.is_a?(Date)
              d
            else
              begin
                Date.parse(d)
              rescue ArgumentError
                pp "Invalid date format: #{d}"
                nil
              end
            end
          rescue ArgumentError
            pp "Invalid date : #{d}"          
          end
        end
     
      if options[:current_start_date].nil? && options[:current_end_date].nil?
        if options[:collection_type] == "backlog"
          if match = max_time_interval.match(/(\d*)\.days/)
            current_start_date = [ current_end_date.prev_day( (match.captures[0].to_i)-1 ) , start_date].max
          end
          # if the start_date is not in the same month as the end date,
          # start from the first day of the month of the end date
          if match = max_time_interval.match(/(\d*)\.month/)
            current_start_date = [(Date.new(current_end_date.year, current_end_date.month, 1) ), start_date].max
          end
          if match = max_time_interval.match(/(\d*)\.year/)
            current_start_date = [(Date.new(current_end_date.year, 1, 1) ), start_date].max
          end
        end
  
        if options[:current_start_date].nil? && options[:current_end_date].nil?
          current_end_date = current_end_date + 1 unless current_end_date == Date.today
          if match = max_time_interval.match(/(\d*)\.days/)
            current_start_date = current_start_date + 1
          end
        end
  
      else
        
        if current_start_date == start_date
          return { current_start_date: nil, current_end_date: nil }
        end
        current_end_date = current_start_date
  
        if match = max_time_interval.match(/(\d*)\.days/)
          current_start_date = [ current_end_date.prev_day(match.captures[0].to_i) , start_date].max
        end
        if match = max_time_interval.match(/(\d*)\.month/)
          current_start_date = [ current_end_date.prev_month(match.captures[0].to_i) , start_date].max
        end
        if match = max_time_interval.match(/(\d*)\.year/)
          current_start_date = [ current_end_date.prev_year(match.captures[0].to_i) , start_date].max
        end
      end
     
      return { current_start_date: current_start_date.strftime(format), current_end_date: current_end_date.strftime(format) }
    end  

    def update_config_with_query_data( query:{}, options:{})
      # Process {{<values>}} in the config with Mustache.render
      # replace {{query.<values>}} with the values from this query
      replacements = options.clone
      begin
        @config = @config.map { |k, v| 
          unless @init_config[k].nil?
            v = @init_config[k].clone()
          end
          replacements[k] = v
          [k, v]
        }.to_h

        if replacements[:date].nil?
          replacements[:date]  = Time.now.strftime("%Y/%m/%d")  
          if replacements[:collection_type] == "recent_records"
            replacements[:date]       = Time.now.strftime("%Y_%m/%d")  
          end
          if replacements[:collection_type] == "backlog"
            if query[:backlog][:current_process_periode].nil?
              backlog_dir_date = query[:backlog][:start_date]
            else
              backlog_dir_date =query[:backlog][:current_process_periode][:current_start_date] || Time.now
            end
            replacements[:date] = "#{Time.now.strftime("%Y-%m-%d")}/backlog/#{  backlog_dir_date.to_datetime.strftime("%Y_%m") }/"
          end
        end
        if replacements[:records_dir_date].nil?
          replacements[:records_dir_date] = Time.now.strftime("%Y/%m/%d")
        end

        replacements[:today] = Time.now.strftime("%Y/%m/%d")
        replacements[:year]  = Time.now.strftime("%Y")
        replacements[:month] = Time.now.strftime("%m")
        replacements[:day]   = Time.now.strftime("%d")
        replacements[:hour]  = Time.now.strftime("%H")

        @config = JSON.parse( Mustache.render(JSON.generate(@config), replacements),  :symbolize_names => true)

        @config[:last_parsing_datetime] = get_parsing_datetime( query: query )
      rescue Exception => e
        raise e
      end
    end

    def get_parsing_datetime( query:{} )
      # puts  query[:last_parsing_datetime] 
      # puts "get_parsing_datetime"      
      # puts  @command_line_options[:last_parsing_datetime]
      return Time.parse( @command_line_options[:last_parsing_datetime] ) unless  @command_line_options[:last_parsing_datetime].nil?
      return query[:last_parsing_datetime] if query[:last_parsing_datetime].is_a?(Time)
      return Time.parse( query[:last_parsing_datetime] ) unless query[:last_parsing_datetime].nil? || query[:last_parsing_datetime].empty?
      return Time.parse("2000/01/01")
    end
  
=begin
    def handle_current_process_config( query: {}, options: {})
    begin
      pp "====================================>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>"
      pp query
      unless query[:current_process_url].nil?
        @logger.warn ("Previous download [#{ options[:collection_type] }] not finished properly. current_process_url still available in query config #{ @config[:query][:query][:id]} : #{ options[:internal_collector_id] }") 
        @config[ options[:download_url_prop].to_sym ] = query[:current_process_url]
        unless query[:current_process_periode].nil?
          @config[:start_date] = query[:current_process_periode][:current_start_date]
          @config[:end_date]   = query[:current_process_periode][:current_end_date]
        end
      end
    rescue Exception => e
      @logger.error ("Error in handle_current_process_config: #{e.message}")
      @logger.error ("Error in handle_current_process_config [query]: #{query}")
      @logger.error ("Error in handle_current_process_config [options]: #{options}")
      exit();
    end 
  end 
=end

    def update_query_config
      begin
        tmp_query_parameters = [:current_process_periode, :current_process_url]
        if @command_line_options[:last_parsing_datetime].nil?
          new_queries = @query_config[:queries].map { |q| 
            #new_q = @queries_to_process.select{ |ptop| ptop[:query][:id] == q[:query][:id] }.first
            new_q = @queries_to_process.select{ |ptop| ptop[:internal_collector_id] == q[:internal_collector_id] }.first
            unless new_q.nil?
              p = proc { |v1, v2| 
                result = {}
                if v1.class != v2.class
                  raise "#{v1} and #{v2} are not of the same type"
                end
                v1.each do |k, v|
                  if tmp_query_parameters.include?(k) && v2[k].nil?
                    next
                  end
                  unless v2[k].nil?
                    if v1[k].is_a?(Hash)
                      v = p.call(v1[k],v2[k])
                    else
                      v = v2[k]
                    end
                  end
                  if v.is_a?(Date) || v.is_a?(DateTime)
                    v= v.strftime()
                  end
                  result.store(k.to_sym, v)
                end
                tmp_query_parameters.each do |k|
                  unless v2[k].nil?
                    result.store(k, v2[k])
                  end
                end 
                result
              }
              q = p.call(q,new_q)
            end
            q
          }
          @query_config[:queries] = new_queries
        end
      rescue Exception => e
        @logger.error ("Error in update_query_config: #{e.message}")
      end
    end

    def prepare_query(icandid_config: nil, query: nil, options: {})
      begin

        @logger.debug ("prepare_query for collection_type #{ options[:collection_type] } ")
        @logger.debug ("prepare_query with url parameter: #{  options[:download_url_prop] } ")
        
        if options[:collection_type] == "backlog"
          @logger.warn ("Previous backlog download not finished properly. current_process_url still available in query config #{ @config[:query][:query][:id]}  : #{ query[:query][:internal_collector_id] }") 
          unless query[:backlog].nil?
            unless query[:backlog][:current_process_url].nil?
              update_config_with_query_data( query: query, options: options )
              @config[ options[:download_url_prop].to_sym ] = query[:backlog][:current_process_url]
              unless query[:backlog][:current_process_periode].nil?
                @config[:start_date] = query[:backlog][:current_process_periode][:current_start_date]
                @config[:end_date]   = query[:backlog][:current_process_periode][:current_end_date]
              end
              return nil
            end
          end
        else
          unless query[:recent_records].nil?
            unless query[:recent_records][:current_process_url].nil?
              @logger.warn ("Previous download not finished properly. current_process_url still available in query config #{ @config[:query][:query][:id]}")
              update_config_with_query_data( query: query, options: options )
              @config[ options[:download_url_prop].to_sym ] = query[:recent_records][:current_process_url]
              unless query[:recent_records][:current_process_periode].nil?
                @config[:start_date] = query[:recent_records][:current_process_periode][:current_start_date]
                @config[:end_date]   = query[:recent_records][:current_process_periode][:current_end_date]
              end
              return nil
            end
          end
        end
                
        if  @config[ options[:download_url_prop].to_sym ].nil?
          raise "#{  options[:download_url_prop]  } not available in config"
        end

        if options[:collection_type] != "backlog" && options[:collection_type] != "recent_records"
          # No need for calculating periodes. Just download once. 
          # Used for datasets without a periodicaly download expl. IMDB or plenum
          update_config_with_query_data( query: query, options: options )
          return nil
        end

        if options[:collection_type] == "backlog"
          query[:backlog][:current_process_url] = @config[ options[:download_url_prop].to_sym ]
        else
          query[:recent_records][:current_process_url] = @config[ options[:download_url_prop].to_sym ]
        end

        if options[:collection_type] == "backlog"
          options[:start_date] = query[:backlog][:start_date]
          options[:end_date] = query[:backlog][:end_date]
          unless query[:backlog][:current_process_periode].nil?
            options[:current_start_date] = query[:backlog][:current_process_periode][:current_start_date]
            options[:current_end_date] = query[:backlog][:current_process_periode][:current_end_date]
          end
        end
        if options[:collection_type] == "recent_records"
          if query[:recent_records][:last_run_update].nil?
            if query[:backlog][:end_date].nil?
              start_date = Date.new(Date.today.year)
            else
              start_date = Date.parse(query[:backlog][:end_date])
            end
          else
            start_date =  Date.parse(query[:recent_records][:last_run_update])
            if start_date > Date.today
              start_date = Date.today
            end 
          end
          options[:start_date] = start_date
          options[:end_date] = Date.today
        end
        format = options[:format] || "%Y%m%d"
        # start_date : The lower bound of record creation time in UTC ( "20210102" )
        # end_date   : The upper bound of record creation time in UTC ( "20210123" )
        # page       : The page number of the result set to return. ( 1 )
        @logger.debug ("options #{ options } ")        

        current_process_periode = get_periode(options)


        @logger.debug ("current_process_periode #{ current_process_periode } ")     
      
        current_start_date = current_process_periode[:current_start_date]
        current_end_date = current_process_periode[:current_end_date]
        
        if current_start_date.nil? || current_end_date.nil?
          @logger.info ("No period available. Usually this means everything has been downloaded")
          update_config_with_query_data( query: query, options: options )
          @config[ options[:download_url_prop].to_sym ] = nil
          query[:recent_records][:current_process_periode] = nil
          query[:recent_records][:current_process_url] = nil
          query[:backlog][:current_process_periode] = nil
          query[:backlog][:current_process_url] = nil
          return nil
        end

        if @config[ :rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = @config[:rule_set].constantize 
        end

        @config[:start_date] = current_start_date
        @config[:end_date]   = current_end_date
        #@ingest_data[:dataset][:@id]  = query[:query][:id]
        #@ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()
        #options[:prefixid] = "#{@ingest_data[:prefixid]}_#{ @ingest_data[:provider][:@id].downcase }_#{@ingest_data[:dataset][:@id].downcase }"
        #options[:start_date] = current_start_date
        #options[:end_date]   = current_end_date

        if options[:collection_type] == "backlog"
          query[:backlog][:current_process_periode] = current_process_periode
        else
          query[:recent_records][:current_process_periode] = current_process_periode
        end

        # update_config_with_query_data: variable substitution for @config[ options[:download_url_prop].to_sym ]
        # before adding it to the query properties
        update_config_with_query_data( query: query, options: options )
        
        if options[:collection_type] == "backlog"
          query[:backlog][:current_process_url] = @config[ options[:download_url_prop].to_sym ]
        else
          query[:recent_records][:current_process_url] = @config[ options[:download_url_prop].to_sym ]
        end

        update_query_config()

      rescue Exception => e
        @logger.error ("Error in prepare_query: #{e.message}")
        exit();
      end
    end
  end
end