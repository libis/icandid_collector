#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'

include Icandid

skip_recent = true

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/Twitter/'
RECORDS_DIR  = '/records/Twitter/'

#############################################################
TESTING = false
STATUS = "downloading"
RESTART_PROCESS_TIME = "09:00"
# stop retrying backlogprocess if the time is betwee RESTART_PROCESS_TIME - 15min and RESTART_PROCESS_TIME
# otherwise the recent_saarch will have to wait untill the backlog it is completely finished

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


begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/twitter/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/twitter/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
 
  collector = IcandidCollector::Input.new(icandid_config.config) 

  @logger.info ("Start downloading using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("downloading for queries in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  # Alwyas get the recent records first. After that start processing the backlog
  # All query[:recent_records][:url] are nil and all query[:recent_records][:last_run_update] have te value today: recent_records has been processed for today 

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

  unless skip_recent
    icandid_config.query_config[:queries].each.with_index() do |query, index|

      @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
      if query[:recent_records].nil?
        # No recent_records for this query
        @logger.info ("recent_records not configured for this query")
        next
      end

      options = { :collection_type => "recent_records", :query =>  query[:query] }
      source_records_dir = icandid_config.get_source_records_dir( options: options)
      
      @logger.info ("downloads written to #{ source_records_dir }")

      unless query[:recent_records][:url].nil? || query[:recent_records][:url].empty? 
        # previous process was not finished
        @logger.warn "Continue were the previous unfinished process ended"
        url = query[:recent_records][:url]
      else
        if query[:recent_records][:last_run_update].nil?
          start_time = query[:backlog][:start_date].to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        else
          start_time = query[:recent_records][:last_run_update].to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
        end
    
        current_process_date = Time.now.strftime("%Y-%m-%dT00:00:00Z")
        # recent_records has been processed for today
        if start_time === current_process_date
          next
        end
    
        url_options[:current_process_date] = current_process_date,
        url_options[:start_time]           = start_time,
        url_options[:next_token]           = ""
    
        if url_options[:query_max_results] > 100 
          url_options[:query_max_results] = 100 
        end

        puts    url_options[:start_time]
    
        unless query[:recent_records][:next_token].nil? || query[:recent_records][:next_token].empty? 
          url_options[:next_token] = "next_token=#{query[:recent_records][:next_token]}"
        end

        url = icandid_config::create_recent_url( url: icandid_config.config[:recent_url], query: query, options: url_options)
      end

      while url 
        data = collector.get_data(url, url_options)

        if data.nil?
          @logger.warn "NO DATA AVAILABLE on this url #{url}"
          break
        end

        unless data["errors"].nil?
          if data["errors"].select { |e| e["message"] =~ /Invalid 'start_time':'(.*)'. 'start_time' must be on or after (.*)/  }.empty?

            errors = filter_twitter_error( data["errors"] )
            unless errors.empty?
              url=nil
              puts data
              raise "Error in request"
            end
          else
            old_time = $1.to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            new_time = ($2.to_date + 1).strftime("%Y-%m-%dT%H:%M:%SZ")
            @logger.warn ("replace start date #{old_time} ==> #{new_time} ")
            url_options[:start_time]           = new_time
            #url.gsub! old_time, new_time
            url = icandid_config::create_url( url: icandid_config.config[:recent_url], query: query, options: url_options)
            next
          end
        end

        if data["meta"]["result_count"] == 0
            @logger.warn "NO RESULTS AVAILABLE for this query: #{url}"
            break
        end

        filename = "#{query[:query][:id]}_#{data["meta"]["oldest_id"]}_#{data["meta"]["newest_id"]}"
        output.to_jsonfile( data, filename, source_records_dir , true )

        @logger.debug ("add next_token to recent_records config #{ data["meta"]["next_token"].to_s } ")
        if data["meta"]["next_token"].to_s.nil? || data["meta"]["next_token"].to_s.empty? 
          url_options[:next_token] = ""
          url = nil
        else
          url_options[:next_token] = "next_token=#{ data["meta"]["next_token"].to_s }"
          url = icandid_config::create_url( url: icandid_config.config[:recent_url], query: query, options: url_options)
        end

        url = nil if TESTING

        query[:recent_records][:next_token]=data["meta"]["next_token"].to_s
        query[:recent_records][:url]=url

        collector.retries = 0
        icandid_config::update_query_config(query: query, index: index)
        
        # only 1 request / second
        sleep 1
        
        
      end

      query[:recent_records][:last_run_update] = start_process
      icandid_config::update_query_config(query: query, index: index)

    end
  end

puts "-----------------------------------------------------------------------------------------------"
sleep(4)
  ################## BACK LOG ######################################

  # hoe voorkomen dat telkens de eerste query wordt verwerkt !!!!
  # Check altijd eerst of de query[:backlog][:url] leeg is. 
  # Als dat niet zo is, was dit de plaats waar het process de vorige keer werd onderbroken.
  # filter alle queries met query[:backlog][:completed]  == false
  # Als alle query[:backlog][:completed] == false zijn start dan bij de eerste queries
  # check alle current_process_date en neem de meest recente. 
  # Als de query[:backlog][:current_process_date] kleiner is dan (verder terug in de tijd) de meest recente current_process_date
  # Ga dan naar de volgende query
  # Bij het starten van een backlog 
  #  - query[:backlog][:current_process_date] ==> - 1 month
  #  - query[:backlog][:completed]  ==> false
  # Als de next_url leeg is query[:backlog][:completed]  ==> true

  # make shure every query backlog has an current_process_date if it is not completed
  icandid_config.query_config[:queries].each.with_index() do |query, index|
    unless query[:backlog][:completed]
      raise " query[:query][:backlog][:start_date] missing" if query[:backlog][:start_date].nil? 
      if query[:backlog][:end_date].nil? 
        if query[:backlog][:current_process_date].nil?
          query[:backlog][:end_date] = start_process.to_date.strftime("%Y-%m-%dT00:00:00Z")
          icandid_config::update_query_config(query: query, index: index)
        else
          raise " query[:query][:backlog][:end_date] missing" if query[:backlog][:end_date].nil? 
        end
      end
      # backlog is processed from recent to oldest
      if query[:backlog][:current_process_date].nil?
        query[:backlog][:current_process_date] = query[:backlog][:end_date]
        icandid_config::update_query_config(query: query, index: index)
      end
    end
  end

  # detect most recent process_dates
  current_process_dates = icandid_config.query_config[:queries].map{ |q| q[:backlog][:current_process_date].to_date }
  current_running_process = icandid_config.query_config[:queries].select{ |q| !q[:backlog][:url].nil? }
  earliest_start_date = icandid_config.query_config[:queries].map{ |q| q[:backlog][:start_date].to_date }.min

  if (current_process_dates.uniq.size == 1 && current_running_process.size == 0) 
    @logger.info ("All backlog records for #{ current_process_dates[0] } are processed")
    current_process_dates = [ (current_process_dates[0] - 1.month).end_of_month ]
  end
  most_recent_process_date = current_process_dates.max

  if icandid_config.query_config[:queries].select { |q| !q[:backlog][:completed] }.empty?
    @logger.info ("All backlog records are processed")
  else

  while most_recent_process_date > (earliest_start_date - 1.days)
    @logger.debug ("Processing backlog records for current_process_date : #{ most_recent_process_date }")
    

    icandid_config.query_config[:queries].each.with_index() do |query, index|
      
      @logger.debug ("Processing backlog records for #{ query[:query][:name]  } [ #{query[:query][:id]} ]")

      if query[:backlog].nil?
        @logger.info ("backlog processing not configured for this query")
        next
      end

      if query[:backlog][:completed]
        @logger.info ("backlog processing already completed for #{ query[:query][:name]  } [ #{query[:query][:id]} ]")
        next
      end
  
      current_process_date = query[:backlog][:current_process_date].to_date 
      @logger.debug ("current_process_date : #{ current_process_date }")
      @logger.debug ("most_recent_process_date : #{ most_recent_process_date }")
      if current_process_date < (query[:backlog][:start_date].to_date)
        query[:backlog][:completed] = true
        icandid_config::update_query_config(query: query, index: index)
        @logger.info ("Backlog is complete for #{ query[:query][:name]  } [ #{query[:query][:id]} ]")
        @logger.info (" current_process_date : #{ current_process_date  } <  query[:backlog][:start_date] #{query[:backlog][:start_date]}")
        next
      end
      if current_process_date < most_recent_process_date 
        @logger.info ("First process the queries with current_process_date after or equal to #{ most_recent_process_date } !")
        next
      end

      options = { :collection_type => "backlog", :query =>  query[:query] , :backlog =>  query[:backlog]}
      source_records_dir = icandid_config.get_source_records_dir( options: options)

      @logger.info ("downloads written to #{ source_records_dir }")

      # url_options[:start_time] = query[:backlog][:current_process_date].to_date.beginning_of_month.strftime("%Y-%m-%dT%H:%M:%SZ")
      url_options[:start_time] = query[:backlog][:current_process_date].to_date.end_of_month.strftime("%Y-%m-%dT%H:%M:%SZ")
      url_options[:end_time]   = ((query[:backlog][:current_process_date].to_date.end_of_month)+1).strftime("%Y-%m-%dT%H:%M:%SZ")

      if url_options[:end_time] > query[:backlog][:end_date]
        url_options[:start_time]   = ((query[:backlog][:current_process_date].to_date)).strftime("%Y-%m-%dT00:00:00Z")
        url_options[:end_time]   = ((query[:backlog][:current_process_date].to_date)+1).strftime("%Y-%m-%dT00:00:00Z")
      end

      if url_options[:query_max_results] > 100 
        url_options[:query_max_results] = 100 
      end
  
      unless query[:backlog][:next_token].nil? || query[:backlog][:next_token].empty? 
        url_options[:next_token] = "next_token=#{query[:backlog][:next_token]}"
      else
        url_options[:next_token] = ''
      end

      url = icandid_config::create_backlog_url( url: icandid_config.config[:backlog_url], query: query, options: url_options)

      while url
        if  ( Time.parse(RESTART_PROCESS_TIME) - (60*15) ) < Time.now  && Time.now  <= Time.parse(RESTART_PROCESS_TIME) 
          icandid_config.update_system_status("ready")
          raise "Stop processing backlog. the process is about to restart via crontab"
        end

        data = collector.get_data(url, url_options)

        if data.nil?
          @logger.warn "NO DATA AVAILABLE on this url #{url}"
          break
        end
  
        unless data["errors"].nil?
          if data["errors"].select { |e| e["message"] =~ /Invalid 'start_time':'(.*)'. 'start_time' must be on or after (.*)/  }.empty?
  
            errors = filter_twitter_error( data["errors"] )
            unless errors.empty?
              url=nil
              puts data
              raise "Error in request"
            end
          else
            old_time = $1.to_date.strftime("%Y-%m-%dT%H:%M:%SZ")
            new_time = ($2.to_date + 1).strftime("%Y-%m-%dT%H:%M:%SZ")
            @logger.warn ("replace start date #{old_time} ==> #{new_time} ")
            url_options[:start_time]           = new_time
            #url.gsub! old_time, new_time
	    # only 1 request / second
            sleep 5
            url = icandid_config::create_url( url: icandid_config.config[:backlog_url], query: query, options: url_options)
            next
          end
        end

        filename = "#{query[:query][:id]}_#{data["meta"]["oldest_id"]}_#{data["meta"]["newest_id"]}"
        output.to_jsonfile( data, filename, source_records_dir , true )

        @logger.debug ("add next_token to backlog config #{ data["meta"]["next_token"].to_s } ")
        if data["meta"]["next_token"].to_s.nil? || data["meta"]["next_token"].to_s.empty? 
          url_options[:next_token] = ""
          url = nil
        else
          url_options[:next_token] = "next_token=#{ data["meta"]["next_token"].to_s }"
          url = icandid_config::create_url( url: icandid_config.config[:backlog_url], query: query, options: url_options)
        end

        url = nil if TESTING

        query[:backlog][:next_token]=data["meta"]["next_token"].to_s
        query[:backlog][:url]=url
        icandid_config::update_query_config(query: query, index: index)

        collector.retries = 0

        # only 1 request / second
        sleep 1

      end

      query[:backlog][:last_run_update] = start_process
      next_process_date = (current_process_date - 1.month).end_of_month.strftime("%Y-%m-%d")

      if next_process_date.to_date < (query[:backlog][:start_date].to_date)
        query[:backlog][:completed] = true
        @logger.info ("Backlog is complete for #{ query[:query][:name]  } [ #{query[:query][:id]} ]")
        @logger.info (" next_process_date : #{ next_process_date  } <  query[:backlog][:start_date] #{query[:backlog][:start_date]}")
      else
        query[:backlog][:current_process_date] = next_process_date
      end

      icandid_config::update_query_config(query: query, index: index)

    end

    break if TESTING

    most_recent_process_date = (most_recent_process_date - 1.month).end_of_month
  end

  end
  
  icandid_config.update_system_status("ready")

rescue => exception
  @logger.error("Error : #{ exception } ")
  @logger.info("Twitter download is finished with errors")
ensure
  @logger.info("Twitter download is finished without errors")
end

=begin
Elke provider kan meerder datasets aanleveren
Elke dataset heeft een 
  - backlog
  - daily, weekly, monthly, quartly, yearly update

In de config moet dus worden bijgehouden voor welke dataset welke records reeds zijn afgehaald.
==> provider : config.yml
==> dataset(s) : queries.yml
Indien mogelijk zal de directory structuur eerst de dataset en daarna downloaddate bevatten.
De directory structuur van de backlog is gebasseerd op de publicationdate ????
De directory structuur van de updates is gebasseerd op de downloaddate ????
Voor download is het source_records/<provider>/<dataset>/<download_date>/ eventueel opgesplits in backlog en updates
De geparste records zouden beste ook in een directory structuur komen die gebasseerd is op de process time (sorteren op input date !!! publication date is maar een veld zoals een ander.)

=end

