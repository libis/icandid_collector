#encoding: UTF-8
require 'yaml'
require 'logger'
require 'zip'
require 'data_collector'
require 'timeout'

include DataCollector::Core

module IcandidCollector
  class Input 

    attr_accessor :config, :retries

    def initialize(config, url_options = {})
      @logger = Logger.new(STDOUT)
      @config = config

      @retries = 0
    end

    def login(url_options)

      @logger.info ("Get new tokens trough credentials")
      
      auth = @config[:auth]

      uri = auth[:login_url]

      uri.scan(/\{\{([^{}]*)\}\}/).each { |substitution|
        substitution = substitution[0]
        if  auth[substitution.to_sym].nil?
          raise ("Missing option \"#{ substitution }\" to substitute uri")
        else
          uri = uri.gsub(/\{\{#{substitution}\}\}/,auth[substitution.to_sym])
        end
      }

      uri = URI(uri)

      http = Net::HTTP.new(uri.host, uri.port)
      http.use_ssl = true
      http.read_timeout = 10
      http.verify_mode = OpenSSL::SSL::VERIFY_NONE
      request = Net::HTTP::Post.new(uri)

      if login_request_body = auth[:login_request_body]
        login_request_body.scan(/\{\{([^{}]*)\}\}/).each { |substitution|
          substitution = substitution[0]
          if  auth[substitution.to_sym].nil?
            raise ("Missing option \"#{ substitution }\" to substitute login_request_body")
          else
            login_request_body = login_request_body.gsub(/\{\{#{substitution}\}\}/, auth[substitution.to_sym])
          end
        }
        request.body = login_request_body
      end
      http_response = http.request(request)            

      if http_response.code === '401'
          raise "Unable to login received status code = #{http_response.code}"
          return false
      end
      
      auth[:access_token] = JSON.parse( http_response.body.to_s )["access_token"]
      auth[:bearer_token] = JSON.parse( http_response.body.to_s )["access_token"]
      auth[:refresh_token] =JSON.parse( http_response.body.to_s )["refresh_token"]
      @config[:auth] = auth

      return true
    end


    def get_data(url, url_options)
      begin
        number_of_retries = url_options[:number_of_retries] || 2
        @retry_count = 0 if @retry_count.nil?
        timing_start = Time.now
       
        @url_options = url_options

        data = input.from_uri(url, url_options)
        # pp data
        unless data.nil? || data["error"].nil?
          @logger.error("Data response: [#{ data["error"]["code"]}] #{ data["error"]["message"]}")  
          case data["error"]["code"].to_i
          when 400
            data = JSON.parse( input.raw )
          when 401
              @logger.info ("Authentication failed:")
              if login(url_options)
                  @logger.info ("Config file updated with new tokens")
                  @logger.info ("Recall the get_request")
                  url_options[:bearer_token] =  @config[:auth][:bearer_token]
                  data = get_data(url, url_options)
              end
          when 404
            @logger.info ("Not found:")
            @logger.info ("#{url} returned 404")
            raise "Not found"
          when 429 
            if @retry_count < number_of_retries
              @retry_count += 1
              @logger.error ("Wait 300 seconds and try Again ==> number_of_retries:#{@retry_count}")
              sleep 300
              #data = get_data(url, url_options)
              #@raw = input.raw
              data = get_data(url, url_options)
              @retry_count -= 1
            else
              @logger.error("429 and to many redirects")
              @logger.error( input.raw )
              raise "429 and to many redirects"
            end
          when 503 
            if @retry_count < number_of_retries
              @retry_count += 1
              @logger.error ("Wait 15 seconds and try Again ==> number_of_retries:#{@retry_count}")
              sleep 15
              #data = get_data(url, url_options)
              #@raw = input.raw
              data = get_data(url, url_options)
            end                
          else
            @logger.error( input.raw )
            raise "API request failed"
          end
        end
        #pp data
        data
      rescue Timeout::Error => exc
        @logger.error("Error [timed out]: #{exc.message}")
        if @retry_count < number_of_retries
          @retry_count += 1
          @logger.error ("Wait 300 seconds and try Again ==> number_of_retries:#{@retry_count}")
          sleep 300
          @retry_count -= 1
          get_data(url, url_options)
        end
      rescue => exception
        @logger.error("Error : #{ exception } ")
        raise "Error get_data !!!! ==> number_of_retries:#{@retry_count}"
      end
    end

    def parse_data( file: "", options: {}, rule_set: nil )
      begin
        if rule_set.nil?
          raise "rule_set is required to parse file"
        end
        output.clear()
        data = input.from_uri("file://#{ file }", {} )
                
        @logger.debug(" options #{ options }")

        #@logger.debug(" rules_ng.run #{ rule_set }")
        #puts rule_set
        #puts rule_set[:version]
        #puts "================>"

        rules_ng.run( rule_set[:rs_records], data, output, options )

        #pp output.raw
        
        #output.crush
        
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

    def convert_data( rule_set: nil , options: {} )
      begin
        if rule_set.nil?
          raise "rule_set is required to parse file"
        end
        
        converted_data = DataCollector::Output.new
        converted_data.clear

        data = JSON.parse(output.raw.to_json)

        rules_ng.run( rule_set[:rs_records], data, converted_data, options)
        
        converted_data[:records]

      rescue StandardError => e
        @logger.error("Error convert data  #{rule_set} ")  
        @logger.error("#{ e.message  }")
        @logger.error("#{ e.backtrace.inspect   }")
        @logger.error( "HELP, HELP !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!")
        raise e
        exit
      end
    end


    def csv_file_to_hash(file, seprator=",", encoding="UTF-8")
      begin
          @raw = rdata = File.read("#{file}", :encoding => encoding).scrub
  
          #@logger.debug("csv_file_to_hash #{encoding} #{file}") 
          orig_encoding = rdata.encoding
          rdata.force_encoding("UTF-8")
          unless rdata.valid_encoding?
            raise (" file encoding has invalid UTF-8")
          end
  
  #        rdata = rdata.gsub('\"', "'")
          data = CSV.parse(rdata, headers: true, col_sep: seprator)
  
          data
      rescue StandardError => msg
          puts "Error csv_file_to_hash: unable to read CSV #{file}"
          puts "msg: #{msg}"
          {}
      end
    end

    def unzip_file (file, destination)
      @logger.info("Unzip #{file}") 
      Zip::ZipFile.open(file) do |zip_file|
        
        @logger.info("Unzip zip_file #{zip_file}") 
  
          zip_file.each do |f|
              f_path = File.join(destination, f.name)
              FileUtils.mkdir_p(File.dirname(f_path))
              f.extract(f_path) unless File.exist?(f_path)
          end
      end
    end


    def write_records(records_dir: nil, record_format: 'json', file_name: nil,  clear_output: true, options: {})
      if records_dir.nil?
        raise "no records_dir specified !"
      end

      # records_dir = "#{records_dir}"
      @logger.debug(" Output to folder:  #{records_dir}")        
      @logger.debug("  record_format:  #{record_format}")        
      
      unless output[:records].nil?
        if output[:records].is_a?(Array)
          records = output[:records]
        else
          records = [ output[:records] ]
        end
        
        unless file_name.nil?
          record_file_name = file_name
        end

        if record_format == 'csv'
          record_file_name = "csv_output.csv" if file_name.nil?
          write_csv(records: records, record_file_name: record_file_name, records_dir: records_dir, clear_output: clear_output,  options: options)
        else
          records.each do |record|
            record_file_name = record[:@id] if file_name.nil?
            write_file(record: record, record_file_name: record_file_name, records_dir: records_dir, record_format: record_format, clear_output: clear_output)
          end
        end

      end
    end

    def write_csv(records: , record_file_name: nil, records_dir: , clear_output: true,  options: {})
      if records_dir.nil?
        raise "no records_dir specified !"
      end
      #if @config[:csv_headers].nil?
      #  raise "no csv_headers specified !"
      #end

      csv_headers = options[:csv_headers].keys

      # puts 'HEADERS:'
      # puts csv_headers

      csv_file = File.join(records_dir,record_file_name)

      if File.exists?( csv_file )
        header_written = false
      else
        header_written = true
      end
      
      CSV.open(csv_file, "ab", :write_headers=> header_written, :headers => csv_headers, :col_sep => "\t") do |csv|
        records.each { |record| csv << collect_csv_values(record, csv_headers) }
      end

    end

    def write_file(record: , record_file_name: nil, records_dir:, record_format: 'json', clear_output: true)
      if record.nil?
        raise "no record specified !"
      end
      if records_dir.nil?
        raise "no records_dir specified !"
      end

      if @config[:dir_based_on_datePublished]
        if record[:datePublished].nil?
            raise "record[:datePublished] may not be nil if @dir_based_on_datePublished  is true"
        end
        records_dir = records_dir.gsub(/\{\{record_dataPublished\}\}/, record[:datePublished].to_datetime.year.to_s)
      end

      if record_format == 'json'
        output.to_jsonfile( record, record_file_name, records_dir, @config[:override_parsed_records])
      elsif record_format == 'xml'
        raise "Ouput to xml not implemented yet!!!!"
      else
        output.to_jsonfile( record, record_file_name, records_dir, @config[:override_parsed_records])
      end
      if clear_output
        output.clear()
      end
    end

    def extract_fulltext_with_tika( id:, data: )
      ts = Time.now.to_f
      result = ''
      tika_url = "https://#{@config[:tika_server]}/tika"


      #request.body_stream = File.open("#{file}")
      #request.content_length = File.size("#{file}")
      #request.content_type = "text/plain" # ?!
      # HTTP.post(URL, body: File.open("example.json"))

      f_data = HTTP.put(tika_url, headers: { accept: "text/plain" }, body: data)
      if f_data.code == 200
        result = f_data.body.to_s.encode!('UTF-8', :undef => :replace, :invalid => :replace, :replace => "")

        if ( ( result.scan(/\ufffd/).length.to_f / result.length.to_f ) > 0.20 )
        #  char_hash={}
        #  result.split('').each { |c| 
        #    # c = c.ord
        #    if char_hash.has_key?(c)
        #      char_hash[c] += 1 
        #    else
        #      char_hash[c] = 1
        #    end
        #  }
          result = "Extraction text from pdf fails! encoding issues - Unicode mapping"
        end
        result.to_json
      end
      puts "\textract_fulltext_with_tika - #{Time.now.to_f - ts} s"
      result
    rescue Exception => e
      puts "extract_fulltext_with_tika - #{id} - #{e.message}"
    end 

    
    def get_fulltext_from_url( url:, options: nil )
      ts = Time.now.to_f

      f_data = HTTP.get(url)

      if f_data.code == 200 && f_data.headers["Content-Type"] == "text/html;charset=UTF-8" && f_data.headers["Content-Transfer-Encoding"] == "binary"
        
        result = f_data.body.to_s.encode!('UTF-8', :undef => :replace, :invalid => :replace, :replace => "")
        if ( ( result.scan(/\ufffd/).length.to_f / result.length.to_f ) > 0.20 )
        #  char_hash={}
        #  result.split('').each { |c| 
        #    # c = c.ord
        #    if char_hash.has_key?(c)
        #      char_hash[c] += 1 
        #    else
        #      char_hash[c] = 1
        #    end
        #  }
          result = "get text from url fails! encoding issues - Unicode mapping"
        end
        result.to_json
      end
      puts "\get_fulltext_from_url - #{Time.now.to_f - ts} s"
      result
    rescue Exception => e
      puts "get_fulltext_from_url - #{options[:id]} - #{e.message}"
    end 

    def collect_csv_values(hash, headers)
      arr = headers.map { | header | 
        value = hash[header]
        if value.nil?
          ""
        elsif (value.class == Array)
          value.join(',').delete("\n")
        elsif (value.class == String)
          value.delete("\n")
        elsif (value.class == Integer)
          value 
        else
          pp value
          value
        end
      } 
      arr
    end

  end
end
