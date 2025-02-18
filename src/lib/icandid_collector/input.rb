#encoding: UTF-8

module IcandidCollector

  class Input

    attr_accessor :icandid_config,  :raw, :total_nr_parsed_files

    def initialize( icandid_config: {} )
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::DEBUG
      @icandid_config = icandid_config
      @total_nr_parsed_files = 0
      @retries = 0
      @number_of_retries = 0
    end

    def collect_data_from_uri ( url: nil, options: {} )
      begin
        if url.nil?
          raise "url is required to collect_data_from_uri"
        end

        if options[:method].nil?
          options[:method] = "GET"
        end

        @number_of_retries = 2
        unless options[:number_of_retries].nil?
          @number_of_retries = options[:number_of_retries]
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
        elsif  /^Unable to process received status code = 429/ =~ e.message
          pp "@retries #{@retries}"
          pp "number_of_retries #{@number_of_retries}"
          if @retries < @number_of_retries
            @retries += 1
            @logger.error ("Wait 600 seconds and try Again ==> number_of_retries:#{@retry_count}")
            sleep 600
            collect_data_from_uri(url: url,  options: options )
          else
            @logger.error("429 Too Many Requests")
            @logger.error( data )
            raise "429 Too Many Requests"
          end
        else
          raise e.message
        end
      end

    end

    def download_file_from_uri ( url: nil, download_path: nil , options: {} )
      begin
        if url.nil?
          raise "url is required to download_file_from_uri"
        end
        if download_path.nil?
          raise "download_path is required to collect_datdownload_file_from_uria_from_uri"
        end
        
        if options[:method].nil?
          options[:method] = "GET"
        end

        @number_of_retries = 2
        unless options[:number_of_retries].nil?
          @number_of_retries = options[:number_of_retries]
        end
        
        uri = URI.decode_www_form_component("#{url.to_s}")
        uri = URI(uri)
        url = uri.to_s

        http = HTTP
        ctx = nil
        http_query_options = {}

        if options.key?(:headers)
          @logger.debug "Set http headers"
          http = http.headers(options[:headers])
        end

        if options.key?(:method) && options[:method].downcase.eql?('post')
          raise DataCollector::InputError, "No body found, a POST request needs a body" unless options.key?(:body)
          http_query_options[:body] = options[:body]
  
          http_response = http.follow.post(url, http_query_options)
        elsif options.key?(:method) && options[:method].downcase.eql?('put')
          raise DataCollector::InputError, "No body found, a PUT request needs a body" unless options.key?(:body)
          http_query_options[:body] = options[:body]
  
          http_response = http.follow.put(url, http_query_options)
        else
          http_response = http.follow.get(url, http_query_options)
        end

        case http_response.code
        when 200..299
          
          header_filename = get_filename(http_response.headers)
          #unless header_filename.nil?
          #  download_path =  File.join(File.dirname(download_path), header_filename)
          #end
          
          file_type = options.with_indifferent_access.has_key?(:content_type) ? options.with_indifferent_access[:content_type] : get_file_type(http_response.headers)
          #pp file_type
          #unless options.with_indifferent_access.has_key?(:raw) && options.with_indifferent_access[:raw] == true
          #  case file_type
          #  when 'application/ld+json'
          #   pp "save_to #{download_path}"
          #  when /^image/
          #    pp "save_to #{download_path}"
          #  else
          #    pp "save_to #{download_path}"
          #  end 
          #end
          #pp "TEST TEST TEST ----------------------------"

          File.open(download_path, 'wb') { |file| file.write(http_response.body) }
          raise '206 Partial Content' if http_response.code == 206
          return { download_path: download_path, content_type: file_type, header_filename: header_filename }

        when 401
          raise DataCollector::InputError, 'Unauthorized'
        when 403
          raise DataCollector::InputError, 'Forbidden'
        when 404
          raise DataCollector::InputError, 'Not found'
        else
          raise DataCollector::InputError, "Unable to process received status code = #{http_response.code} error= #{http_response.body.to_s}"
        end              
 
      rescue Exception => e
        @logger.error ("Error in download_file_from_uri: #{e.message}")

        if @retries < @number_of_retries
          @retries += 1
          @logger.error ("Wait 30 seconds and try Again ==> number_of_retries:#{ @retries }")
          sleep 30
          download_file_from_uri( url: url, download_path: download_path , options: options )
        end
        @logger.error("Already tried #{@retries} times, I give up")
        raise DataCollector::InputError, "Unable to download file"
      end
    end

    def process_files( options: {} )
      config = @icandid_config.config()
      
      if config[:rule_set].nil?
        raise "rule_set is required to parse file"
      end

      files = get_files_to_parse()

      if files.empty?
        @logger.warn ("No files to process in #{ @icandid_config.config[:source_records_dir] }")        
      end
      options[:config] =  @icandid_config.config()
      options[:ingest_data] =  @icandid_config.ingest_data()

      @logger.info ("Start parsing using rule_set: #{ config[:rule_set]}")
      config[:nbr_created_records] = 0
      files.each_with_index do |source_file, index| 
        # pp source_file
        parse_data( file: source_file, options: options, rule_set: config[:rule_set].constantize )
        @total_nr_parsed_files =  @total_nr_parsed_files + 1
        output.data[:records] = [output.data[:records]] unless output.data[:records].is_a?(Array)

        one_record_output = DataCollector::Output.new

        # @logger.debug ("process data output.data #{ output.data } ")

        output.data[:records].each do | data |

          unless data.nil?
            data = data.with_indifferent_access
            
            config[:nbr_created_records] = config[:nbr_created_records] + 1

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
        source_records_dir:       @icandid_config.config[:source_records_dir].strip,
        source_file_name_pattern: @icandid_config.config[:source_file_name_pattern].strip,
        last_parsing_datetime:    @icandid_config.config[:query][:last_parsing_datetime].strip
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
          if Regexp.new(source_file_name_pattern).match(File.basename(source_file))
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

        #   pp data

        # @logger.debug(" options #{ options }")
        # @logger.debug("parse_data rules_ng.run #{ rule_set }")
        
        rules_ng.run( rule_set[:rs_records], data, output, options )

        # output.crush
        # @logger.debug("parse_data output  #{ output}")
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

    def get_file_type(headers)
      file_type = 'application/octet-stream'
      file_type = if headers.include?('Content-Type')
                    headers['Content-Type'].split(';').first
                  else
                    @logger.debug "No Header content-type available"
                    MIME::Types.of(filename_from(headers)).first.content_type
                  end
        return file_type
    end

    def get_filename(headers)
      filename = if headers.include?('Content-Disposition')
                    content_disposition_hash = Hash[  headers['Content-Disposition'].delete('\\"').split(';').map { |e| e.strip.split('=', 2) } ]
                    if content_disposition_hash.include?('filename')
                      content_disposition_hash["filename"]
                    else
                      nil
                    end
                  else
                   nil
                  end
        return filename
    end
  end
end