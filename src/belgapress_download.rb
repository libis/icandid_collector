#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
# require 'belgapress_utils'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/BelgaPress/'
RECORDS_DIR  = '/records/BelgaPress/'

#############################################################
TESTING = false
STATUS = "downloading"

RESTART_PROCESS_TIME = "07:30"
STOP_BACKLOG_PROCESS_TIME = "04:00"

# stop retrying backlogprocess if the time is betwee RESTART_PROCESS_TIME - 15min and RESTART_PROCESS_TIME
# otherwise the recent_saarch will have to wait untill the backlog it is completely finished
#COUNT = 10
#count = 30 # Best ratio for limit 900 records in 15 min (1 request for newsobjects and 30 request newsobjects/<uuid> 29*31=899 request / 899-29 =870 records ) 
#count = 50 # Best ratio for limit 900 records in 15 min (1 request for newsobjects and 50 request newsobjects/<uuid> 17*51=850 request / 867-17 = 850 records ) 

#COUNT = 100 # Best ratio for limit 900 records in 15 min (1 request for newsobjects and 50 request newsobjects/<uuid> 17*51=850 request / 867-17 = 850 records ) 
COUNT = 100 


begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/BelgaPress/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/BelgaPress/'),
    :query_config_file => "queries.yml"
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
    :next_token        => '',
    :bearer_token      => icandid_config.config[:auth][:bearer_token],
    :number_of_retries => 25,
    :headers            => { 
      "content-type"    => 'application/x-www-form-urlencoded',
      "X-Belga-Context" => "API" 
    }
  }

  ################## RECENT SEARCH ######################################
  icandid_config.query_config[:queries].each.with_index() do |query, index|

    unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
      @logger.info ("NExt next")
        next
    end   
    @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    if query[:recent_records].nil?
      # No recent_records for this query
      @logger.info ("recent_records not configured for this query")
      next
    end

    # recent_search records are downloaded to {{query_name}}/{{date}}/" 
    # - query_name is tanslitarted from query[:query][:name]
    # - date is download day (today) %Y_%m/%d
    options = { :collection_type => "recent_records", :query =>  query[:query] }
    source_records_dir = icandid_config.get_source_records_dir( options: options)
    
    @logger.info ("downloads written to #{ source_records_dir }")

    if query[:recent_records][:last_run_update].nil?
      if query[:recent_records][:start_date].nil?
        start_time = query[:backlog][:start_date].to_date.strftime("%Y-%m-%d")
      else
        start_time = query[:recent_records][:start_date]
      end
    else
      start_time = query[:recent_records][:last_run_update].to_date.strftime("%Y-%m-%d")
    end
    
    current_process_date = Time.now.strftime("%Y-%m-%d")
    # recent_records has been processed for today
    if start_time === current_process_date
      next
    end

    if start_time < ( (Date.today - 14).strftime("%Y-%m-%d") )
      @logger.warn "Start Date must be not more then 14 days ago #{start_time}" 
      start_time =  (Date.today - 14).strftime("%Y-%m-%d")
    end

    url_options[:current_process_date] = current_process_date,
    url_options[:end_date]             = current_process_date,
    url_options[:start_date]           = start_time,
    url_options[:next_token]           = "",
    url_options[:COUNT]                = COUNT

    url = icandid_config::create_recent_url( url: icandid_config.config[:recent_url], query: query, options: url_options)

    while url


      url_options[:bearer_token] = icandid_config.config[:auth][:access_token]
      data = collector.get_data(url, url_options)
      
      if data.nil?
        @logger.warn "NO DATA AVAILABLE on this url #{url}"
        break
      end

      unless (data["data"].empty? && data["_meta"]["total"] == 0)
          @logger.debug ("total record for this query : #{ data["_meta"]}")
          # Expand resultsdata to records with body
          data["data"].map!{ |d|
            url_options[:uuid] = d["uuid"]
            record_url =  icandid_config::create_record_url( url: icandid_config.config[:record_url], query: query, options: url_options)
            record_data = collector.get_data(record_url, url_options)
            unless record_data.empty?
              record_data
            else
              d
            end
          }   
      end


      data["data"].compact!

      unless (data["data"].empty? && data["_meta"]["total"] == 0)
        filename = "#{data["data"].first["uuid"]}_#{data["data"].last["uuid"]}.json"
        output.to_jsonfile( data["data"], filename, source_records_dir , true )
      end

      @logger.debug ("next : #{ data["_links"]["next"] }")
      url = data["_links"]["next"]

      url = nil if TESTING

      query[:recent_records][:url] = url 
      icandid_config::update_query_config(query: query, index: index)


      collector.retries = 0

    end
    query[:recent_records][:last_run_update] = start_process
    icandid_config::update_query_config(query: query, index: index)
  end


  ################## BACK LOG ######################################

  # hoe voorkomen dat telkens de eerste query wordt verwerkt !!!!
  # Check altijd eerst of de query[:backlog][:url] leeg is. 
  # Als dat niet zo is, was dit de plaats waar het process de vorige keer werd onderbroken.
  # filter eerst alle queries met query[:backlog][:completed]  == false
  # Als alle query[:backlog][:completed] == true zijn start dan bij de eerste queries
  # check alle current_process_date en neem de meest recente. 
  # Als de query[:backlog][:current_process_date] kleiner is dan (verder terug in de tijd) de meest recente current_process_date
  # Ga dan naar de volgende query
  # Bij het starten van een backlog 
  #  - query[:backlog][:current_process_date] ==> - 1 month
  #  - query[:backlog][:completed]  ==> false
  # Als de next_url leeg is query[:backlog][:completed]  ==> true


  # make sure every query backlog has a current_process_date if it is not completed
  icandid_config.query_config[:queries].each.with_index() do |query, index|
    unless query[:backlog][:completed]
      raise " query[:query][:backlog][:start_date] missing" if query[:backlog][:start_date].nil? 
      raise " query[:query][:backlog][:end_date] missing" if query[:backlog][:end_date].nil? 
      # backlog is processed from recent to oldest
      if query[:backlog][:current_process_date].nil?
        query[:backlog][:current_process_date] = query[:backlog][:end_date]
        icandid_config::update_query_config(query: query, index: index)
      end
    end
  end

  not_completed_backlogs = icandid_config.query_config[:queries].select { |q| !q[:backlog][:completed] }
  if not_completed_backlogs.size == 0
    @logger.info ("All backlog are processed [COMPLETED]")
    icandid_config.update_system_status("ready")    
    exit
  end


  current_process_dates = (icandid_config.query_config[:queries].select{ |q| ! q[:backlog][:completed] }).map { |q|  pp q[:backlog]; q[:backlog][:current_process_date].to_date }
  current_running_process = icandid_config.query_config[:queries].select{ |q| !q[:backlog][:url].nil? }
  earliest_start_date     = icandid_config.query_config[:queries].map   { |q| q[:backlog][:start_date].to_date }.min 

  
#  firts_backlog_processing = icandid_config.query_config[:queries].select{ |q| !q[:backlog][:completed] && q[:backlog][:current_process_date] == q[:backlog][:end_date] }
#  if (current_process_dates.uniq.size == 1 && current_running_process.size == 0) 
#    unless firts_backlog_processing
#      @logger.info ("All backlog records for #{ current_process_dates[0] } are processed")
#      current_process_dates = [ (current_process_dates[0] - 1.month).end_of_month ]
#    end
#  end

  most_recent_process_date = current_process_dates.max

  while most_recent_process_date > (earliest_start_date - 1.days)
    @logger.debug ("Processing backlog records for current_process_date : #{ most_recent_process_date }")

    puts most_recent_process_date


    icandid_config.query_config[:queries].each.with_index() do |query, index|

      unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
        @logger.info ("Next next")
          next
      end

      if query[:backlog].nil?
        @logger.info ("No backlog processing for #{ query[:query][:name]  } [ #{query[:query][:id]} ] !!!!!!!")
        next
      end
      if query[:backlog][:completed]
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
        @logger.info ("First process the queries with current_process_date after or equal to #{ most_recent_process_date }!!!!!!")
        next
      end

      # backlog records are downloaded to {{query_name}}/{{date}}/" 
      # - query_name is tanslitarted from query[:query][:name]
      # - date is download day (today) %Y-%m-%d/backlog/#{current_process_date.strftime("%Y_%m") 
      options = { :collection_type => "backlog", :query =>  query[:query] , :backlog =>  query[:backlog]}
      source_records_dir = icandid_config.get_source_records_dir( options: options)

      @logger.debug ("downloads written to #{ source_records_dir }")

      url_options[:current_process_date] = current_process_date,
      url_options[:end_date]             = current_process_date.end_of_month,
      url_options[:start_date]           = current_process_date.beginning_of_month,
      url_options[:next_token]           = "",
      url_options[:COUNT]                = COUNT

      if query[:backlog][:end_date].to_date < current_process_date.end_of_month
        url_options[:end_date]  = current_process_date
      end

      url = icandid_config::create_backlog_url( url: icandid_config.config[:backlog_url], query: query, options: url_options)

      while url

        if  ( Time.parse(RESTART_PROCESS_TIME) - (60*15) ) < Time.now  && Time.now  <= Time.parse(RESTART_PROCESS_TIME) 
          icandid_config.update_system_status("ready")    
          raise "Stop processing backlog. the process is about to restart via crontab"
        end

        if  ( Time.parse(STOP_BACKLOG_PROCESS_TIME) - (60*15) ) < Time.now  && Time.now  <= Time.parse(STOP_BACKLOG_PROCESS_TIME)
          icandid_config.update_system_status("ready")
          raise "Stop processing backlog. the process is about to restart via crontab (STOP_BACKLOG_PROCESS_TIME)"
        end

        url_options[:bearer_token] = icandid_config.config[:auth][:access_token]
        data = collector.get_data(url, url_options)

        if data.nil?
          @logger.warn "NO DATA AVAILABLE on this url #{url}"
          break
        end
      

        unless (data["data"].empty? && data["_meta"]["total"] == 0)
          @logger.debug ("total record for this query : #{ data["_meta"]}")
          # Expand resultsdata to records with body
          data["data"].map!{ |d|
            url_options[:uuid] = d["uuid"]
            record_url =  icandid_config::create_record_url( url: icandid_config.config[:record_url], query: query, options: url_options)
            record_data = collector.get_data(record_url, url_options)
            unless record_data.empty?
              record_data
            else
              d
            end
          }   
        end

        unless (data["data"].empty? && data["_meta"]["total"] == 0)
          filename = "#{data["data"].first["uuid"]}_#{data["data"].last["uuid"]}.json"
          output.to_jsonfile( data["data"], filename, source_records_dir , true )
        end

        @logger.debug ("next : #{ data["_links"]["next"] }")
        url = data["_links"]["next"]

        url = nil if TESTING

        query[:backlog][:url] = url 
        icandid_config::update_query_config(query: query, index: index)

        collector.retries = 0
        
      end
      query[:backlog][:last_run_update] = start_process
      query[:backlog][:current_process_date] = (current_process_date - 1.month).end_of_month.strftime("%Y-%m-%d")
      icandid_config::update_query_config(query: query, index: index)
    end
    
    break if TESTING

    most_recent_process_date = (most_recent_process_date - 1.month).end_of_month
  end

  icandid_config.update_system_status("ready")
  
rescue => exception
  @logger.error("Error : #{ exception } ")
ensure
  puts "Todo : send mail ?"
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







