#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
# require 'VlaamsParlement_utils'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "download"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/VlaamsParlement/'
RECORDS_DIR  = '/records/VlaamsParlement/'

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

@already_downloaded_ids =[]

def get_parlementair_initiatief_ids_from_volledige_vergadering(icandid_config, url, url_options, source_records_dir)
  collector = IcandidCollector::Input.new( icandid_config.config ) 
  data = collector.get_data(url, url_options)     

  parlementair_initiatief_ids = []

  if data.nil?
    @logger.warn "NO DATA AVAILABLE on this url #{url}"
    return nil
  end

  if data["items"].nil?
    @logger.warn "NO data[\"items\"]. on this url #{url}"
    return nil
  end

  unless data["items"].empty?
    @logger.debug ("total record for this query (vergadering): #{ data["items"].size }")
    # Expand resultsdata to records with body
    parlementair_initiatief_ids = data["items"].map{ |d|
      pp "vergadering -- agenda-item"
        d["vergadering"]["agenda-item"].map!{ |agenda_item| 
          pp "agenda-lijn"
            agenda_item["agenda-lijn"].map!{ |agenda_lijn| 
              pp "parlementair-initiatief"
                agenda_lijn["parlementair-initiatief"].map!{ |parlementair_initiatief| 
                    parlementair_initiatief["id"].to_s
                }
            }
        }
    }
    parlementair_initiatief_ids.flatten!.sort!.uniq!
    
    @logger.debug ("total parlementair_initiatief for this query : #{ parlementair_initiatief_ids.size }")

    if  url_options[:type] == "backlog"
        @logger.debug ("already previously downloaded parlementair_initiatief : #{ (parlementair_initiatief_ids & @already_downloaded_ids).size }")
        parlementair_initiatief_ids =  parlementair_initiatief_ids - @already_downloaded_ids 
    end
  end
  
  return parlementair_initiatief_ids  
  
end

def get_parlementair_initiatief_ids_from_vergadering(icandid_config, url, url_options, source_records_dir)
  collector = IcandidCollector::Input.new( icandid_config.config ) 
  data = collector.get_data(url, url_options)     

  parlementair_initiatief_ids = []

  if data.nil?
    @logger.warn "NO DATA AVAILABLE on this url #{url}"
    return nil
  end

  if data["items"].nil?
    @logger.warn "NO data[\"items\"]. on this url #{url}"
    return nil
  end

  unless data["items"].empty?
    @logger.debug ("total record for this query (vergadering): #{ data["items"].size }")
    parlementair_initiatief_ids = data["items"].map{ |d|
#      pp "vergadering_id: #{d["vergadering"]["id"]}"
      url_options[:uuid] = d["vergadering"]["id"]
      url = icandid_config::create_url( url: icandid_config.config[:meeting_url], query: {}, options: url_options)

      vergadering = collector.get_data(url, url_options)
      pi_ids = vergadering["vergadering"]["agenda-item"].map!{ |agenda_item| 
#        pp "agenda-lijn"
        agenda_item["agenda-lijn"].map!{ |agenda_lijn| 
#          pp "parlementair-initiatief"
          agenda_lijn["parlementair-initiatief"].map!{ |parlementair_initiatief| 
              parlementair_initiatief["id"].to_s
          }
        }
      }
      pi_ids = pi_ids + vergadering["vergadering"]["journaallijn"].map!{ |journaallijn| 
        journaallijn["parlementair-initiatief"].map!{ |parlementair_initiatief| 
          parlementair_initiatief["id"].to_s
        }
      }

      pi_ids.flatten.sort.uniq
    }
#    pp parlementair_initiatief_ids
    parlementair_initiatief_ids = parlementair_initiatief_ids.flatten.compact.sort.uniq
    
    @logger.debug ("total parlementair_initiatief for this query : #{ parlementair_initiatief_ids.size }")

    if  url_options[:type] == "backlog"
        @logger.debug ("already previously downloaded parlementair_initiatief : #{ (@already_downloaded_ids).size }")
        parlementair_initiatief_ids =  parlementair_initiatief_ids - @already_downloaded_ids 
    end
  end
  
  return parlementair_initiatief_ids  
end


def get_parlementair_initiatief_ids_from_commissie(icandid_config, url, url_options, source_records_dir)
=begin  
  http://ws.vlpar.be/e/opendata/leg/alle
  ==> items.naam: "2019-2024"
  
  http://ws.vlpar.be/e/opendata/comm/legislatuur?legislatuur=2004-2009
  ==> items.id: 393958
  
  http://ws.vlpar.be/e/opendata/comm/1333265/alle-stvz
  ==> items.commissie-status.parlementair-initiatief [ id ]
=end

  collector = IcandidCollector::Input.new( icandid_config.config ) 
  data = collector.get_data(url, url_options)     

  parlementair_initiatief_ids = []

  if data.nil?
    @logger.warn "NO DATA AVAILABLE on this url #{url}"
    return nil
  end

  if data["items"].nil?
    @logger.warn "NO data[\"items\"]. on this url #{url}"
    return nil
  end

  unless data["items"].empty?
    @logger.debug ("total record for this query (commissie) : #{ data["items"].size }")
    parlementair_initiatief_ids = data["items"].map{ |d|
      pp "commissie: #{d["commissie"]["id"]}"
      url_options[:uuid] = d["commissie"]["id"]
      url = icandid_config::create_url( url: icandid_config.config[:committee_url], query: {}, options: url_options)

      committee = collector.get_data(url, url_options)

      pi_ids = committee["items"].map!{ |item| 
        item["commissie-status"]["parlementair-initiatief"].map!{ |parlementair_initiatief| 
          parlementair_initiatief["id"].to_s
        }
      }
      
      pi_ids.flatten.sort.uniq
    }
#    pp parlementair_initiatief_ids
    parlementair_initiatief_ids = parlementair_initiatief_ids.flatten.compact.sort.uniq
    
    @logger.debug ("total parlementair_initiatief for this query : #{ parlementair_initiatief_ids.size }")

    @logger.debug ("already previously downloaded parlementair_initiatief : #{ (@already_downloaded_ids).size }")
    parlementair_initiatief_ids =  parlementair_initiatief_ids - @already_downloaded_ids 
  end
   
  return parlementair_initiatief_ids  
end


def get_legislatures(icandid_config, url_options)

  collector = IcandidCollector::Input.new( icandid_config.config ) 
  url = icandid_config::create_url( url: icandid_config.config[:legislatures_url], query: {}, options: url_options)
  legislatures = collector.get_data(url, url_options)
      
  return legislatures["items"].map { |l| l["legislatuur"]["naam"]}

end

def download_records(icandid_config, url, url_options, source_records_dir, parlementair_initiatief_ids)

  collector = IcandidCollector::Input.new( icandid_config.config ) 
  # pp parlementair_initiatief_ids

  #docs = parlementair_initiatief_ids[0..0].map { |parlementair_initiatief_id|
  docs = parlementair_initiatief_ids.map { |parlementair_initiatief_id|

    unless @already_downloaded_ids.include?(parlementair_initiatief_id)
      url_options[:uuid] = parlementair_initiatief_id
      pi_url = icandid_config::create_url( url: icandid_config.config[:pi_url], query: {}, options: url_options)
      pi_data = collector.get_data(pi_url, url_options)
      
      # pp pi_data.keys
      
      unless pi_data.empty?
        filename = "#{pi_data["id"]}"
        output.to_jsonfile( pi_data, filename, source_records_dir , true )

        unless pi_data["document"].nil?
          unless pi_data["document"].empty?
              @logger.debug ("download #{pi_data["document"]["bestandsnaam"]} for #{pi_data["id"]} ")
              pdf_file = "#{source_records_dir}/#{pi_data["id"]}_#{p pi_data["document"]["bestandsnaam"]}"
              File.open(pdf_file, "wb") do |file|
                  file.write URI.open(pi_data["document"]["url"]).read
              end
          end
        end

        @already_downloaded_ids = @already_downloaded_ids << parlementair_initiatief_id

        unless pi_data["parlementair-initiatief"].nil?
             
          # test = pi_data["parlementair-initiatief"].map { |i|  { :objecttype => i["objecttype"], :titel => i["titel"], :id => i["id"], :document => i["document"] } }
          unless pi_data["parlementair-initiatief"].empty?
            extra_pi_ids = pi_data["parlementair-initiatief"].map{ |pi| "#{pi["id"]}" }
            extra_pi_ids = extra_pi_ids - @already_downloaded_ids
            download_records(icandid_config, url, url_options, source_records_dir, extra_pi_ids)
=begin              
              pi_data["parlementair-initiatief"].each { |pi| 
                  unless pi["document"].nil?
                      @logger.debug ("  download #{pi["document"]["bestandsnaam"]} for #{pi["id"]} ") 
                      pdf_file = "#{source_records_dir}/#{pi["id"]}_#{p pi["document"]["bestandsnaam"]}"
                      File.open(pdf_file, "wb") do |file|
                          file.write URI.open(pi["document"]["url"]).read
                      end
                  end
              }
=end              
          end
        end
      end # unless pi_data.empty?
    end
  }

  return 1
end


begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/VlaamsParlement/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/VlaamsParlement/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )

  @logger.info ("Start downloading using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("downloading for queries in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  # Alwyas get the recent records first. After that start processing the backlog
  # All query[:recent_records][:url] are nil and all query[:recent_records][:last_run_update] have te value today: recent_records has been processed for today 
  url_options = {
    :base_url          => icandid_config.config[:base_url],
    :headers            => { 
      "content-type"    => 'application/json',
    }
  }

  legislatures = get_legislatures(icandid_config, url_options)

  ################## RECENT SEARCH ######################################

  icandid_config.query_config[:queries].each.with_index() do |query, index|

    @logger.info ("downloading recent_records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    if query[:recent_records].nil?
      # No recent_records for this query
      @logger.info ("recent_records not configured for this query")
      next
    end

    if query[:recent_records][:last_run_update].nil?
        if query[:recent_records][:start_date].nil?
          start_time = query[:backlog][:end_date].to_date
        else
          start_time = query[:recent_records][:start_date].to_date
        end
    else
        start_time = query[:recent_records][:last_run_update].to_date
    end


    current_process_date = Time.now
    # keep processing untill the start_time > current_process_date
    # get_parlementair_initiatief_ids_from_vergadering ( request per week of year) 
    while start_time.beginning_of_week <= current_process_date
        
        year = start_time.year.to_s
        weeknr = start_time.cweek.to_s

        @logger.info ("downloading for #{year} weeknr: #{weeknr} ")

        options = { :year_dir => year, :weeknr => weeknr }
        source_records_dir = icandid_config.get_source_records_dir( options: options)
        @logger.info ("downloads written to #{ source_records_dir }")

        url_options[:year] = year
        url_options[:weeknr] = weeknr
        url_options[:type] = "recent"

  
        # url = "https://ws.vlpar.be/e/opendata/verg/zoek?year=2022&weeknr=28"
        url = icandid_config::create_url( url: icandid_config.config[:recent_url], query: {}, options: url_options)
        parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_vergadering(icandid_config, url, url_options, source_records_dir)

        # url = "https://ws.vlpar.be/e/opendata/verg/volledig/zoek?year=2022&weeknr=44"
        # parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_volledige_vergadering(icandid_config, url, url_options, source_records_dir)
        
        return_val = download_records(icandid_config, url, url_options, source_records_dir, parlementair_initiatief_ids)

        if return_val.nil?
            break
        end

        query[:recent_records][:start_date] = start_time 
        icandid_config::update_query_config(query: query, index: index)

        start_time = (start_time + 1.week)

    end


    options = { :year_dir => "**", :weeknr => "**" }
    source_records_dir = icandid_config.get_source_records_dir( options: options)
 
    @already_downloaded_ids = Dir.chdir(icandid_config.config[:source_records_base_dir]) { Dir.glob("**/*.json").map { |path| File.basename(path, ".*") } }
    @already_downloaded_ids.sort!.uniq!

    @logger.debug ("already previously downloaded parlementair_initiatief : #{ (@already_downloaded_ids).size }")

    url_options[:legislatuur_uuid] = legislatures.shift

    # get_parlementair_initiatief_ids_from_commissie ( request for huidige legislatuur) 
    options = { :year_dir => url_options[:legislatuur_uuid], :weeknr => "committees" }
    source_records_dir = icandid_config.get_source_records_dir( options: options)

    url = icandid_config::create_url( url: icandid_config.config[:committees_legislature_url], query: {}, options: url_options)
    parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_commissie(icandid_config, url, url_options, source_records_dir)

    return_val = download_records(icandid_config, url, url_options, source_records_dir, parlementair_initiatief_ids)

    if return_val.nil?
        break
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

      year = current_process_date.year.to_s
      weeknr = current_process_date.cweek.to_s

      @logger.info ("downloading for #{year} weeknr: #{weeknr} ")


      options = { :year_dir => year, :weeknr => weeknr }
      source_records_dir = icandid_config.get_source_records_dir( options: options)
      @logger.info ("downloads written to #{ source_records_dir }")

      url_options[:year] = year
      url_options[:weeknr] = weeknr
      url_options[:type] = "backlog"

      url = icandid_config::create_url( url: icandid_config.config[:backlog_url], query: {}, options: url_options)
      parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_vergadering(icandid_config, url, url_options, source_records_dir)

      # url = "https://ws.vlpar.be/e/opendata/verg/volledig/zoek?year=2022&weeknr=44"
      # parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_volledige_vergadering(icandid_config, url, url_options, source_records_dir)
      
      
      
      return_val = download_records(icandid_config, url, url_options, source_records_dir, parlementair_initiatief_ids)

      if return_val.nil?
          break
      end
   
      query[:backlog][:last_run_update] = start_process
      query[:backlog][:current_process_date] = (current_process_date - 1.week).beginning_of_week.strftime("%Y-%m-%d")
      icandid_config::update_query_config(query: query, index: index)
    end
    
    break if TESTING

    most_recent_process_date = (most_recent_process_date - 1.week).beginning_of_week
  end


  options = { :year_dir => "**", :weeknr => "**" }
  source_records_dir = icandid_config.get_source_records_dir( options: options)

  @already_downloaded_ids = Dir.chdir(icandid_config.config[:source_records_base_dir]) { Dir.glob("**/*.json").map { |path| File.basename(path, ".*") } }
  @already_downloaded_ids.sort!.uniq!

  @logger.debug ("already previously downloaded parlementair_initiatief : #{ (@already_downloaded_ids).size }")

  legislatures.each { |legislature| 
    
    url_options[:legislatuur_uuid] = legislature
    
    # get_parlementair_initiatief_ids_from_commissie ( request for legislatuur) 
    options = { :year_dir => legislature, :weeknr => "committees" }
    source_records_dir = icandid_config.get_source_records_dir( options: options)


    url = icandid_config::create_url( url: icandid_config.config[:committees_legislature_url], query: {}, options: url_options)
    parlementair_initiatief_ids = get_parlementair_initiatief_ids_from_commissie(icandid_config, url, url_options, source_records_dir)

    pp parlementair_initiatief_ids
    return_val = download_records(icandid_config, url, url_options, source_records_dir, parlementair_initiatief_ids)
    
    if return_val.nil?
      break
    end

  }

  icandid_config.update_system_status("ready")

rescue => exception
  @logger.error("Error : #{ exception } ")
ensure
  puts "Todo : send mail ?"
end
