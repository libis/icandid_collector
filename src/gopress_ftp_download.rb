#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
require_relative './lib/ftp_sync'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/GoPress/'
RECORDS_DIR  = '/records/GoPress/'

#############################################################
TESTING = false
STATUS = "downloading"
RESTART_PROCESS_TIME = "13:00"
# stop retrying backlogprocess if the time is betwee RESTART_PROCESS_TIME - 15min and RESTART_PROCESS_TIME
# otherwise the recent_saarch will have to wait untill the backlog it is completely finished

begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/GoPress/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/GoPress/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
 
  collector = IcandidCollector::Input.new(icandid_config.config) 

  @logger.info ("Start downloading using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("downloading for queries in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  # Alwyas get the recent records first. After that start processing the backlog
  # All query[:recent_records][:url] are nil and all query[:recent_records][:last_run_update] have te value today: recent_records has been processed for today 

  ftp_options = {
    :host            => icandid_config.config[:auth][:ftp_host],
    :consumer_key    => icandid_config.config[:auth][:consumer_key],
    :consumer_secret => icandid_config.config[:auth][:consumer_secret],
  }

  ftp = FtpSync.new ftp_options[:host], ftp_options[:consumer_key], ftp_options[:consumer_secret], { passive: true, :verbose => true}

  icandid_config.query_config[:queries].each.with_index() do |query, index|

    @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    if query[:recent_records].nil?
      # No recent_records for this query
      @logger.info ("recent_records not configured for this query")
      next
    end

    options = { :collection_type => "recent_records", :query =>  query[:query] }
    source_records_dir = icandid_config.get_source_records_dir( options: options)

    records_dir_on_server = "/xml/#{query[:query][:value] }/"

    @logger.info ("records_dir_on_server : #{ records_dir_on_server }")

    @logger.info ("downloads written to #{ source_records_dir }")

    ftp.download_zips source_records_dir, records_dir_on_server, :since => true
    
    # only 1 request / second
    sleep 1

    query[:recent_records][:last_run_update] = start_process
    icandid_config::update_query_config(query: query, index: index)

  end
 
  icandid_config.update_system_status("ready")

rescue => exception
  @logger.error("Error : #{ exception } ")
ensure
  puts "Todo : send mail ?"
end
