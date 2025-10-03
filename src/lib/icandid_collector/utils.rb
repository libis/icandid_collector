# encoding: utf-8
require 'net/smtp'

module IcandidCollector 
  
  class Utils
    
    attr_accessor :icandid_config

    def initialize( icandid_config: {} )
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::DEBUG
      @icandid_config = icandid_config
    end

    def tikaFullTextExtraction( data, options ) 
      attempts = 0
      begin
        attempts += 1
        if options.has_key?(:file)
          downloadfile = options[:file]
          if File.file?(downloadfile) 
            # TODO 
            # check if the extraction file (.txt) is older than the source file (.pdf in most cases)
            @logger.debug("get full text from previous extration [#{ downloadfile } (#{ File.mtime(downloadfile) }) ]")
            return File.open(downloadfile, 'r').read
          end
        end

        if icandid_config[:tika_url].nil?
          if icandid_config[:tika_server].nil?
            raise "tika_server missing in configuration"
          else
            icandid_config[:tika_url] = "https://#{ icandid_config[:tika_server] }/tika"
          end
        end
        unless icandid_config[:tika_url].nil?
          tika_response = HTTP.put(icandid_config[:tika_url], headers: { accept: "text/plain" }, body: data)
          if tika_response.code == 200
            output = tika_response.body.to_s.encode!('UTF-8', :undef => :replace, :invalid => :replace, :replace => "")
            if options.has_key?(:file)
              File.open(downloadfile, 'w') { |file| file.write(output) }
            end
            return output
          end
        end
      rescue StandardError => e
        @logger.error("#{ e.message  }")
        if attempts < 5
          @logger.error("Retry [ #{ attempts  } ]")
          retry 
        end
        @logger.error("#{ e.backtrace.inspect   }")
        raise e.message
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
          data.map(&:to_h)
      rescue StandardError => msg
          puts "Error csv_file_to_hash: unable to read CSV #{file}"
          puts "msg: #{msg}"
          {}
      end
    end
    
    def languageDetection( data, options )
      begin
        #@logger.debug("Start detect language #{ options[:language_detection_url]} OR #{ options[:language_detection_service] }")
        if options[:language_detection_url].nil?
          unless options[:language_detection_service].nil?
            options[:language_detection_url] = "https://#{ options[:language_detection_service] }/detect"
          end
        end
        unless options[:language_detection_url].nil?
          http= HTTP
          http = http.headers({
              "Content-Type": "application/json"
          })

          http_response = http.follow.post(options[:language_detection_url], body: {"text": data}.to_json)
          if http_response.status == 200 && !http_response.body.to_s.empty?
              response = JSON.parse(http_response.body.to_s)
              unless response["language"].nil? || response["language"].empty? || response["is_reliable"] == "false"
                  return response["language"]
              end
          end
          
          return "und" # undetermined
        end
      rescue StandardError => e
        @logger.error("#{ e.message  }")
        @logger.error("#{ e.backtrace.inspect   }")
        pp e.message
        raise e.message
      end
    end


    def mailErrorReport (subject,  report , importance, config)
      now = DateTime.now

      unless ENV['SMTP_SERVER'] 
        pp "-!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!-"
        pp " No smtp-server configured"
        pp "-!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!-"
        exit
      end

      @smtp_server = ENV['SMTP_SERVER'] 
      @from_address = ENV['FROM_MAIL_ADDRESS'] 
      @to_address = ENV['ADMIN_MAIL_ADDRESS'] 

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

      Net::SMTP.start(@smtp_server, 25, tls_verify: false)  do |smtp|
          smtp.send_message message,
          @from_address , @to_address
      end
    end
  end

end