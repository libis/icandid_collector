#encoding: UTF-8

module IcandidCollector

  class Input

    attr_accessor :icandid_config,  :raw, :total_nr_parsed_files

    def initialize( icandid_config: {} )
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::DEBUG
      @icandid_config = icandid_config
      @total_nr_parsed_files = 0
    end

    def collect_data_from_uri ( url: nil, options: {} )
      begin
        if url.nil?
          raise "url is required to collect_data_from_uri"
        end

        if options[:method].nil?
          options[:method] = "GET"
        end
        @raw = data = DataCollector::Core.input.from_uri(url, options)
        data

      rescue DataCollector::InputError => e
        if e.message == "Unauthorized"
          config = @icandid_config.config()[:login]
          login_url = config[:url]
          login_options = { method: config[:method] }
          login_options[:headers] = config[:headers]
          login_succeded = false

          if config[:method].nil?
            config[:method] = "GET"
          end
          if config[:method].upcase == "POST"
            login_options[:body] = config[:request_body]
          end

          login_data = collect_data_from_uri(url: login_url, options: login_options )
          
          config[:result_mapping].each { |k,v|
            path = v.split('.').map(&:to_s)
            unless login_data.dig(*path).nil?
              login_succeded = true
              @logger.info ("Retrieved new : #{ path } for config[:auth][#{k.to_sym}]")
              @icandid_config.config()[:auth][k] = login_data.dig(*path)
              if options.has_key?(k)
                options[k] = @icandid_config.config()[:auth][k]
              end
              @icandid_config.update_init_config( key_path: "auth.#{k}" , value: login_data.dig(*path) )
            end
          }

          if login_succeded
            collect_data_from_uri(url: url,  options: options )
          else
            raise e.message
          end
        else
          raise e.message
        end
      end

    end

    def process_files( options: {} )
        config = @icandid_config.config()
      
      if config[:rule_set].nil?
        raise "rule_set is required to parse file"
      end

      files = get_files_to_parse()


      options[:config] =  @icandid_config.config()
      options[:ingest_data] =  @icandid_config.ingest_data()

      @logger.info ("Start parsing using rule_set: #{ config[:rule_set]}")
      files.each_with_index do |source_file, index| 
        # pp source_file
        parse_data( file: source_file, options: options, rule_set: config[:rule_set].constantize )
        @total_nr_parsed_files =  @total_nr_parsed_files + 1
        output.data[:records] = [output.data[:records]] unless output.data[:records].is_a?(Array)

        one_record_output = DataCollector::Output.new

        output.data[:records].each do | data |
          unless data.nil?
            data = data.with_indifferent_access
  
            one_record_output << data
            filename = "#{one_record_output['@id']}.json"
            destination = "file://#{ File.join(config[:records_dir], filename) }"

            one_record_output.to_uri( destination,  options)
            one_record_output.clear
          end
        end
      end    
    end 

    def get_files_to_parse
      @logger.debug ("Get files from: #{ @icandid_config.config[:source_records_dir] } ")
     
      select_files_from_source_records_dir(
        source_records_dir:       @icandid_config.config[:source_records_dir],
        source_file_name_pattern: @icandid_config.config[:source_file_name_pattern],
        last_parsing_datetime:    @icandid_config.config[:query][:last_parsing_datetime] 
      )
    end

    def select_files_from_source_records_dir(source_records_dir: nil, source_file_name_pattern: nil,  last_parsing_datetime: nil )
      files = []
      unless @icandid_config.config[:query][:last_parsing_datetime].nil?
        last_parsing_datetime = Date.parse( @icandid_config.config[:query][:last_parsing_datetime] )
      end

      Dir["#{source_records_dir}/*"].each do |source_file| 

        if File.directory?( source_file )
          if last_parsing_datetime.nil?  || (last_parsing_datetime < File.mtime(source_file))
            files.concat select_files_from_source_records_dir( source_records_dir: source_file, source_file_name_pattern: source_file_name_pattern,  last_parsing_datetime: last_parsing_datetime )
          end
        else
          if Regexp.new(source_file_name_pattern).match(source_file)
            if last_parsing_datetime.nil?  || (last_parsing_datetime < File.mtime(source_file))
              files << source_file
            end
          end
        end
      end
      files.uniq
    end

    def parse_data( file: "", options: {}, rule_set: nil )
      begin
        if rule_set.nil?
          raise "rule_set is required to parse file"
        end

        output.clear()
        #input = DataCollector::Input.new
        #output = DataCollector::Output.new
        data = input.from_uri("file://#{ file }", {} )
        
        options[:file] = file
#        pp data
 #       pp rule_set

        # @logger.debug(" options #{ options }")

        #@logger.debug(" rules_ng.run #{ rule_set }")
        #puts rule_set
        #puts rule_set[:version]
        #puts "================>"

        rules_ng.run( rule_set[:rs_records], data, output, options )

        #pp output.raw
        # output.crush
        
        output

      rescue StandardError => e
        @logger.error("Error parsing file  #{file} ")  
        @logger.error("#{ e.message  }")
        @logger.error("#{ e.backtrace.inspect   }")
        @logger.error( "HELP, HELP !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        raise e
        exit
      end
    end
  end
end