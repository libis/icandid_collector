#encoding: UTF-8
require 'yaml'
require 'logger'
# require 'optparse'
# require 'zip'
# require 'net/smtp'
require 'active_support/all'
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

  class Error < StandardError; end
end