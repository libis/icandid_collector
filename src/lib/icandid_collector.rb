#encoding: UTF-8
require 'yaml'
require 'logger'
require 'optparse'
# require 'zip'
require 'net/smtp'
require 'data_collector'
require 'timeout'

require 'icandid_collector/core'

include DataCollector::Core

module IcandidCollector
  extend DataCollector::Core
  
  ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
  FROM_MAIL_ADDRESS = "icandid@libis.kuleuven.be"
  SMTP_SERVER = "smtp.kuleuven.be"
  ROOT_PATH = File.join( File.dirname(__FILE__), '../../')
  # SOURCE_FILE_NAME_PATTERN = "*.json"

  class Input
    
    attr_accessor :icandid_config

    def initialize( icandid_config: {} )
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
      @icandid_config = icandid_config
    end

    def process_files( options: {} )
  #    pp "--------------------------------------------"
      config = @icandid_config.config()
      
#      pp config

      if config[:rule_set].nil?
        raise "rule_set is required to parse file"
      end

      options[:config] =  @icandid_config.config()
      options[:ingest_data] =  @icandid_config.ingest_data()

      @logger.info ("Start parsing using rule_set: #{ config[:rule_set]}")
      Dir["#{ config[:source_records_dir] }/#{ config[:source_file_name_pattern] }"].each_with_index do |source_file, index| 

        one_record_output = DataCollector::Output.new

        if config[:last_parsing_datetime].nil?
          parse_data( file: source_file, options: options, rule_set: config[:rule_set].constantize )
        else
          if config[:last_parsing_datetime]  < File.mtime(source_file)
            parse_data( file: source_file, options: options, rule_set: config[:rule_set].constantize )
          end
        end
=begin
        if output.raw[:records].is_a?(Array)
          filename = "#{ output.raw[:records].first['@id']  }_#{ output.raw[:records].last['@id'] }.json"
        else
          filename = "#{ output.raw[:records]['@id']}.json"
        end
=end
        output.data[:records] = [output.data[:records]] unless output.data[:records].is_a?(Array)

        output.data[:records].each do | data |

          data = data.with_indifferent_access
          one_record_output << data
          filename = "#{one_record_output['@id']}.json"
          destination = "file://#{ File.join(config[:records_dir], filename) }"
          one_record_output.to_uri( destination,  options)


        end

      end
    
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
        
 #       pp data
 #       pp rule_set

        @logger.debug(" options #{ options }")

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

  class Error < StandardError; end
end