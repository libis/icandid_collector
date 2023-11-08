#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'icandid'
require 'logger'
require 'unidecoder'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

PROVIDER="ZweedsParlement"
ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/ZweedsParlement/'
RECORDS_DIR  = '/records/ZweedsParlement/'
MAX_RESULTS  = 5

#############################################################
TESTING = false
STATUS = "downloading"

def save_results( data: "", source_records_dir: nil )

  unless source_records_dir.nil?
    # get list of uri's to the pdf-documents
    documents = data['dokumentlista']['dokument'].map{ |res| 
      unless res["filbilaga"].nil?
        res["filbilaga"]['fil'].select { |fil| fil["typ"] == "pdf" } 
      end
    }.flatten.compact

    first_id = data['dokumentlista']['dokument'].first["id"].split('/').last
    last_id  = data['dokumentlista']['dokument'].last["id"].split('/').last
    filename = "#{first_id}_#{last_id}".to_ascii

    output.to_jsonfile(data['dokumentlista']['dokument'], filename, source_records_dir , true )

    documents.each { |doc| 
      pdf_file = "#{source_records_dir}#{doc["namn"]}"
      File.open("#{pdf_file}", "wb") do |file|
        http_uri = URI.parse( doc["url"])
        http_uri.scheme = "https"
        data = input.from_uri( http_uri.to_s)
        
        if data["error"].nil?
          file.write data unless data.nil?
        else
          @logger.warn ("No pdf found no this URI #{doc["url"]}")
        end
      end  
    
    }
  end
end

begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/ZweedsParlement/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/ZweedsParlement/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
  
  collector = IcandidCollector::Input.new( icandid_config.config )   

  ingestConfJson =  File.read(File.join(ROOT_PATH, './config/ZweedsParlement/ingest.cfg'))
  INGEST_CONF = JSON.parse(ingestConfJson, :symbolize_names => true)

  @logger.info ("Start downloading using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
 
  @logger.info ("downloading for queries in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  # Alwyas get the recent records first. After that start processing the backlog
  # All query[:recent_records][:url] are nil and all query[:recent_records][:last_run_update] have te value today: recent_records has been processed for today 
  url_options = {
    :base_url          => icandid_config.config[:base_url],
    :opendata_url          => icandid_config.config[:opendata_url],
    :headers            => { 
      "content-type"    => 'application/json',
    }
  }

  ################## RECENT SEARCH ######################################

  icandid_config.query_config[:queries].each.with_index() do |query, index|

    @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    if query[:recent_records].nil?
      # No recent_records for this query
      @logger.info ("recent_records not configured for this query")
      next
    end

    if query[:recent_records][:last_run_update].nil? || query[:recent_records][:last_run_update].empty?
        if query[:recent_records][:start_date].nil?
          start_time = query[:backlog][:end_date].to_date
        else
          start_time = query[:recent_records][:start_date].to_date
        end
    else
        start_time = query[:recent_records][:last_run_update].to_date
    end

    current_process_date = Time.now
      
    @logger.info ("downloading for #{start_time}  ")

    options = { :collection_type => "recent_records", :query =>  query[:query] }
    source_records_dir = icandid_config.get_source_records_dir( options: options)
    @logger.info ("downloads written to #{ source_records_dir }")


    url_options[:end_date]       = ""
    url_options[:start_date]     = start_time
    url_options[:page]           = 1
    url_options[:max_results]    = MAX_RESULTS
    url_options[:aggregation]    = query[:query][:aggregation]
    
    url = icandid_config::create_recent_url( url: icandid_config.config[:recent_url], query: query, options: url_options)

    while url

      data = collector.get_data(url, url_options)
      
      if data.nil?
        @logger.warn "NO DATA AVAILABLE on this url #{url}"
        break
      end
      
      if data['dokumentlista']['dokument'].nil?
        url = nil
      else
        save_results(data:data, source_records_dir:source_records_dir)
        url = data['dokumentlista']['@nasta_sida']
      end

    end 

    query[:recent_records][:last_run_update] = start_process
    icandid_config::update_query_config(query: query, index: index)
  end


  ################## BACK LOG ######################################

  icandid_config.query_config[:queries].each.with_index() do |query, index|
  
    unless query[:backlog][:completed]
      raise " query[:query][:backlog][:start_date] missing" if query[:backlog][:start_date].nil? 
      raise " query[:query][:backlog][:end_date] missing" if query[:backlog][:end_date].nil? 
      # backlog is processed from recent to oldest
      if query[:backlog][:current_process_date].nil? || query[:backlog][:current_process_date].empty?
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

  most_recent_process_date = current_process_dates.max

  while most_recent_process_date > (earliest_start_date - 1.days)
    @logger.debug ("Processing backlog records for current_process_date : #{ most_recent_process_date }")

    icandid_config.query_config[:queries].each.with_index() do |query, index|

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
      url_options[:page]           = 1,
      url_options[:max_results]    = MAX_RESULTS,      
      url_options[:aggregation]    = query[:query][:aggregation],
    
      if query[:backlog][:end_date].to_date < current_process_date.end_of_month
        url_options[:end_date]  = current_process_date
      end

      url = icandid_config::create_backlog_url( url: icandid_config.config[:backlog_url], query: query, options: url_options)

      while url
        data = collector.get_data(url, url_options)

        if data.nil?
          @logger.warn "NO DATA AVAILABLE on this url #{url}"
          break
        end
        if data['dokumentlista']['dokument'].nil?
          url = nil
        else
          save_results(data:data, source_records_dir:source_records_dir)
          url = data['dokumentlista']['@nasta_sida']
        end
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
  @logger.error("#{ exception.message}")
  @logger.error("#{ exception.backtrace.inspect}")
  @logger.info("#{INGEST_CONF[:provider][:name]} download is finished with errors")  

  importance = "High"
  subject = "iCANDID #{INGEST_CONF[:provider][:name]} download"
  message = <<END_OF_MESSAGE
  
  <h2>Error while downloading #{INGEST_CONF[:provider][:name]} data</h2>
  <p>#{exception.message}</p>
  <p>#{exception.backtrace.inspect}</p>
  
  <hr>
  
END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

ensure
  importance = "Normal"
  subject = "iCANDID #{INGEST_CONF[:provider][:name]} download"
  message = <<END_OF_MESSAGE

  <h2>Download #{INGEST_CONF[:provider][:name]} [#{INGEST_CONF[:provider][:@id]}] data</h2>
  Download using config: #{ icandid_config.config.path}/#{ icandid_config.config.file}
<H3>#{$0} </h3>
command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
  <hr>

END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

  @logger.info("#{INGEST_CONF[:provider][:name]} download is finished without errors")

end
