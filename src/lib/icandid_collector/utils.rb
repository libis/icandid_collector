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

    def tikaFullTextExtraction( data ) 
      if icandid_config[:tika_url].nil?
        unless icandid_config[:tika_server].nil?
          icandid_config[:tika_url] = "https://#{ icandid_config[:tika_server] }/tika"
        end
      end
      unless icandid_config[:tika_url].nil?
        f_data = HTTP.put(icandid_config[:tika_url], headers: { accept: "text/plain" }, body: data)
        if f_data.code == 200
          f_data.body.to_s.encode!('UTF-8', :undef => :replace, :invalid => :replace, :replace => "")
        end
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