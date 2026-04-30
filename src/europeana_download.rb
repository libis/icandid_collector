#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
require 'cgi'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ROOT_PATH = File.join( File.dirname(__FILE__), '../')
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/Europeana/'
RECORDS_DIR  = '/records/Europeana/'

#############################################################
TESTING = false
STATUS = "downloading"

CURSOR = "*"

begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/Europeana/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/Europeana/queries/'),
    :query_config_file => "config.yml"
  }

  icandid_config = Icandid::Config.new( config: config )

  collector = IcandidCollector::Input.new( icandid_config.config ) 

  @logger.info ("Start downloading using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("downloading for queries in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  # Alwyas get the recent records first. After that start processing the backlog
  # All query[:recent_records][:url] are nil and all query[:recent_records][:last_run_update] have te value today: recent_records has been processed for today
  url_options = {
    :base_url          => icandid_config.config[:base_url],
    :wskey      => icandid_config.config[:auth][:api_key]
  }

  ################## RECENT SEARCH ######################################
  icandid_config.query_config[:queries].each.with_index() do |query, index|

    @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    if query[:recent_records].nil?
      # No records for this query
      @logger.info ("recent_records not configured for this query")
      next
    end

    # recent_records are downloaded to {{query_name}}/{{date}}/" 
    # - query_name is tanslitarted from query[:query][:name]
    # - date is download day (today) %Y_%m/%d
    query[:name] = query[:query][:name]   

    options = { :collection_type => "recent_records", :query =>  query }
    
    source_records_dir = icandid_config.get_source_records_dir( options: options)

    @logger.info ("downloads written to #{ source_records_dir }")

    current_process_date = Time.now.strftime("%Y-%m-%d")

    url_options[:rows] = 100
    url_options[:cursor] = CURSOR
    url = icandid_config::create_url( url: icandid_config.config[:search_url], query: query, options: url_options)
    data = collector.get_data(url, url_options)
    collector.retries = 0

    if data.nil?
        @logger.warn "NO DATA AVAILABLE on this url #{url}"
        break
    end

    while ( not data["items"].nil?)
      data["items"].each{ |d|
        filename = "Europeana#{d["id"].gsub! "/","-"}"
        fullfilename = File.join(source_records_dir, filename) + ".json"  
        if (not File.file?(fullfilename))
          unless (d["link"].nil?) 
            objectdata = collector.get_data(d["link"])
            d["object"] = objectdata["object"]
            @logger.info("Writing to #{filename}")
            output.to_jsonfile(d, filename, source_records_dir, true)
            sleep(0.4)
            exit if TESTING
          end
        end
      }
	  	
      if (data["nextCursor"].nil?) 
        break
      end
      url_options[:cursor] = CGI::escape(data["nextCursor"])
      sleep(0.4)
      url = icandid_config::create_url( url: icandid_config.config[:search_url], query: query, options: url_options)
      data = collector.get_data(url, url_options)
      collector.retries = 0
    end
	
    query[:recent_records][:last_run_update] = start_process
    icandid_config::update_query_config(query: query, index: index)
  end
end

