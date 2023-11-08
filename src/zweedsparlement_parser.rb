#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'

include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG
@total_nr_parsed_records = 0

ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

Dir[  File.join( ROOT_PATH,"src/rules/zweedsparlement_*.rb") ].each {|file| require file;  }

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "parser"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/'
SOURCE_FILE_NAME_PATTERN = "*.json"

RECORDS_DIR  = '/records/ZweedsParlement/'

ConfJson = File.read(File.join(ROOT_PATH, './config/config.cfg'))
ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)

ingestConfJson =  File.read(File.join(ROOT_PATH, './config/ZweedsParlement/ingest.cfg'))
INGEST_CONF = JSON.parse(ingestConfJson, :symbolize_names => true)

INGEST_CONF[:prefixid] = ICANDID_CONF[:prefixid]

INGEST_CONF[:genericRecordDesc] = "Entry from #{INGEST_CONF[:dataset][:name]}"

#############################################################
TESTING = true
STATUS = "parsing"

begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/ZweedsParlement/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/ZweedsParlement/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
  
  collector = IcandidCollector::Input.new( icandid_config.config ) 

  @logger.info ("Start parsing using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  # https://data.riksdagen.se/sv/koder.html
  # https://data.riksdagen.se/sv/koder/?typ=doktyp&utformat=json

  doktyp   = collector.get_data("https://data.riksdagen.se/sv/koder/?typ=doktyp&utformat=json",{})
  organ    = collector.get_data("https://data.riksdagen.se/sv/koder/?typ=organ&utformat=json",{})
  roll     = collector.get_data("https://data.riksdagen.se/sv/koder/?typ=roll&utformat=json",{})
  riksmote = collector.get_data("https://data.riksdagen.se/sv/koder/?typ=riksmote&utformat=json",{})
  
  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("Parsing for queries in : #{ icandid_config.query_config.path }/#{ icandid_config.query_config.file }")

  rule_set =  icandid_config.config[:rule_set].constantize unless  icandid_config.config[:rule_set].nil?

  @logger.debug ("queries_to_parse: #{ icandid_config.config[:queries_to_parse] }")

  icandid_config.query_config[:queries].each.with_index() do |query, index|

    @logger.info ("Paring records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
    @logger.info ("Start parsing using rule_set: #{icandid_config.config[:rule_set]}")

    unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
      @logger.info ("NExt next")
        next
    end

    INGEST_CONF[:dataset][:@id]  = query[:query][:id]
    INGEST_CONF[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

    # recent_search records are downloaded to {{query_name}}/{{date}}/" 
    # - query_name is tanslitarted from query[:query][:name]
    # - date is download day (today) %Y_%m/%d
    dir_options = { 
        :query =>  query[:query], 
        :date => "**"
    }

    source_records_dir = icandid_config.get_source_records_dir( options: dir_options)
    
    last_parsing_datetime = icandid_config.get_parsing_datetime( query: query )
    source_file_name_pattern = icandid_config.get_file_name_pattern()
   
    @logger.info ("Start parsing query: #{ query[:query][:name] } ")
    @logger.info ("Start parsing source_records_dir: #{source_records_dir} ")
    @logger.info ("Start parsing source_file_name_pattern: #{source_file_name_pattern}")
    @logger.info ("Start parsing last_parsing_datetime: #{last_parsing_datetime}")

    options = {
        :type => "Legislation",
        :prefixid => "#{INGEST_CONF[:prefixid]}_#{ INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }",
        :default_publisher => {
            :@type => "Organization",
            :@id   => "iCANDID_ORGANIZATION_ZWEEDS_PARLEMENT",
            :name  => "Sveriges riksdag"
        },
        :doktyp => doktyp["typer"]["typ"],
        :organ => organ["organ"]["organ"],
        :roll => roll,
        :riksmote => riksmote
    }

    Dir["#{source_records_dir}/#{source_file_name_pattern}"].each_with_index do |source_file, index| 

        if last_parsing_datetime < File.mtime(source_file)
            @logger.debug(" parser - file : #{ source_file }")  

            collector.parse_data( file: source_file, options: options, rule_set: rule_set )

            unless collector.output.raw.empty?
                unless collector.output.raw[:records].kind_of?(Array)
                  collector.output.raw[:records] = [collector.output.raw[:records]]
                end
                collector.output.raw[:records].map do |record|
                  unless record[:text].nil? || record[:text].empty?
                    file = record[:text]
                    data_file = "#{File.dirname(source_file)}/#{file}"
                    if File.exists?(data_file)
                      record[:text] = collector.extract_fulltext_with_tika( id: record[:identifier],  data: File.open(data_file) )
                    else
                      @logger.warn ("PDF-file does not exits for fulltex extraction: #{data_file}")
                    end
                  end
                  record
                end
                                  
                dir_options = { 
                    :query =>  query[:query], 
                    :date => Date.today.strftime("%Y/%m/%d")
                }
              
                if icandid_config.config[:dir_based_on_datePublished]
                    dir_options[:date] = "{{record_dataPublished}}"
                end
  
                @total_nr_parsed_records += 1
    
                collector.write_records( records_dir:  icandid_config.get_records_dir( options:dir_options) )
            end
          
        end
    end

    if  icandid_config.command_line_options[:last_parsing_datetime].nil?
      @logger.info ("Update last_parsing_datetime: #{start_process}")
      query[:last_parsing_datetime] = "#{start_process}"
    else
        @logger.info ("Do not update last_parsing_datetime in config (last_parsing_datetime was a command line options)")
    end
    icandid_config::update_query_config(query: query, index: index)
    
  end
  
  icandid_config.update_system_status("ready")

rescue StandardError => e
  @logger.error("#{ e.message  }")
  @logger.error("#{ e.backtrace.inspect   }")

  importance = "High"
  subject = "[ERROR] iCANDID #{INGEST_CONF[:provider][:name]} parsing"
  message = <<END_OF_MESSAGE
  
  <h2>Error while parsing #{INGEST_CONF[:provider][:name]} data</h2>
  <p>source_file #{source}</p>
  <p>#{e.message}</p>
  <p>#{e.backtrace.inspect}</p>
  
  <hr>
  
END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

  @logger.info("#{INGEST_CONF[:provider][:name]} Parsing is finished with errors")
ensure

  importance = "Normal"
  subject = "iCANDID #{INGEST_CONF[:provider][:name]} parsing [#{@total_nr_parsed_records}]"
  message = <<END_OF_MESSAGE

  <h2>Parsing #{INGEST_CONF[:provider][:name]} [#{INGEST_CONF[:provider][:@id]}] data</h2>
  Parsing using config: #{ icandid_config.config.path}/#{ icandid_config.config.file}
<H3>#{$0} </h3>
command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }

  <hr>

END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)
  @logger.info("#{INGEST_CONF[:provider][:name]} Parsing is finished without errors")

end
