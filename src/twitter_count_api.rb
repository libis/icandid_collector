#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "count"  # used to determine (command line) config options
#############################################################
TESTING = false
STATUS = "counting"
RESTART_PROCESS_TIME = "09:00"
# stop retrying backlogprocess if the time is betwee RESTART_PROCESS_TIME - 15min and RESTART_PROCESS_TIME
# otherwise the recent_saarch will have to wait untill the backlog it is completely finished

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

config = {
    :config_path => File.join(ROOT_PATH, './config/twitter'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/twitter'),
    :query_config_file => "queries.yml"
}

icandid_config = Icandid::Config.new( config: config )

collector = IcandidCollector::Input.new(icandid_config.config) 

start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")

url_options = {
    :base_url             => icandid_config.config[:base_url],
    :next_token         => '',
    :query_expansions   => icandid_config.config[:api_request_params][:expansions],
    :query_tweet_fields => icandid_config.config[:api_request_params][:tweet_fields],
    :query_media_fields => icandid_config.config[:api_request_params][:media_fields],
    :query_place_fields => icandid_config.config[:api_request_params][:place_fields],
    :query_poll_fields  => icandid_config.config[:api_request_params][:poll_fields],
    :query_user_fields  => icandid_config.config[:api_request_params][:user_fields],
    :query_max_results  => icandid_config.config[:api_request_params][:max_results],
    :bearer_token      => icandid_config.config[:auth][:bearer_token],
    :number_of_retries => 5
}

def filter_twitter_error(errors)
    puts "============================================================="
    pp errors
    puts "============================================================="
    errors.select! { |e| 
        !(e["title"] == "Forbidden" && e["resource_type"] == "user") &&
        !(e["title"] == "Not Found Error" && e["resource_type"] == "user") &&
        !(e["title"] == "Not Found Error" && e["resource_type"] == "place") &&
        !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "referenced_tweets.id") &&
        !(e["title"] == "Authorization Error" && e["resource_type"] == "tweet")
    }
    errors
end

counter=0
begin


  @logger.info ("Start counting tweets using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")
 
  @logger.info ("Counting tweets for queries in : #{ icandid_config.query_config.path }/#{ icandid_config.query_config.file }")

 
  icandid_config.query_config[:queries].each.with_index() do |query, index|

    unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
      @logger.info ("NExt next")
      next
    end

    @logger.info ("count for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    
    start_time = query[:backlog][:start_date].to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    if query[:recent_records].nil?
        end_time = query[:backlog][:end_date].to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
    else
        end_time = Time.now.strftime("%Y-%m-%dT00:00:00Z")
    end

    url_options[:start_time]         = start_time
    url_options[:end_time]           = end_time
    url_options[:next_token]         = ""

    url = icandid_config::create_url( url: icandid_config.config[:count_url], query: query, options: url_options)

    while url 
      data = collector.get_data(url, url_options)

      if data.nil?
        @logger.warn "NO DATA AVAILABLE on this url #{url}"
        break
      end

      unless data["errors"].nil?
        errors = filter_twitter_error( data["errors"] )
        unless errors.empty?
        url=nil
        puts data
        raise "Error in request"
        end
      end

      if data["meta"]["total_tweet_count"] == 0
          @logger.warn "NO RESULTS AVAILABLE for this query: #{url}"
          break
      end

      @logger.debug ("total_tweet_count in this response:  #{  data["meta"]["total_tweet_count"] }")

      counter = counter + data["meta"]["total_tweet_count"]

      @logger.debug ("total_tweet_count of processed request: #{ counter }")

      @logger.debug ("add next_token to recent_records config #{ data["meta"]["next_token"].to_s } ")

      if data["meta"]["next_token"].to_s.nil? || data["meta"]["next_token"].to_s.empty? 
        url_options[:next_token] = ""
        url = nil
      else
        url_options[:next_token] = "next_token=#{ data["meta"]["next_token"].to_s }"
        url = icandid_config::create_url( url: icandid_config.config[:count_url], query: query, options: url_options)
      end

      url = nil if TESTING

      collector.retries = 0
     
      puts counter
      # only 1 request / second
      sleep 1
      
    end

    puts "total_tweet_count start_time: #{start_time} - #{end_time} : #{counter}"

  end
  
  icandid_config.update_system_status("ready")

rescue => exception
  @logger.error("Error : #{ exception } ")
  @logger.info("Twitter counting is finished with errors")
ensure
  @logger.info("Twitter counting is finished without errors")
end

