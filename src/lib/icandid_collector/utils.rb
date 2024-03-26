# encoding: utf-8
require 'net/smtp'

module IcandidCollector 
  
  class Utils
    
    attr_accessor :mail_to, :mail_from, :smtp_server

    def initialize( mail_to: ADMIN_MAIL_ADDRESS, mail_from: FROM_MAIL_ADDRESS, smtp_server: SMTP_SERVER)
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::DEBUG
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

      Net::SMTP.start(@smtp_server, 25, tls_verify: false)
          smtp.send_message message,
          @from_address , @to_address
      end
    end
  end

end