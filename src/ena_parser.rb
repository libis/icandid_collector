#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'
require_relative './rules/ena_v1.0'
# require 'belgapress_utils'

include Icandid


@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# ConfJson = File.read( File.join(ROOT_PATH, './config/config.cfg') )
# ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)
PROCESS_TYPE  = "parser"  # used to determine (command line) config options
SOURCE_DIR   = '/source_records/ENA/'
SOURCE_FILE_NAME_PATTERN = "*.csv"

RECORDS_DIR  = '/records/ENA/'

ConfJson = File.read(File.join(ROOT_PATH, './config/config.cfg'))
ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)

ingestConfJson =  File.read(File.join(ROOT_PATH, './config/ENA/ingest.cfg'))
INGEST_CONF = JSON.parse(ingestConfJson, :symbolize_names => true)

INGEST_CONF[:prefixid] = ICANDID_CONF[:prefixid]
INGEST_CONF[:genericRecordDesc] = "Entry from #{INGEST_CONF[:dataset][:name]}"

#############################################################
TESTING = true
STATUS = "parsing"

begin
  config = {
    :config_path => File.join(ROOT_PATH, './config/ENA/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/ENA/'),
    :query_config_file => "periodes.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
  
  collector = IcandidCollector::Input.new( icandid_config.config )

  @TV_codebook =  collector.csv_file_to_hash("#{SOURCE_DIR}/#{icandid_config.config[:themavariabelen]}", ";")
  toplevel = ''

  @TV_codebook.each_with_index{ |line,index| 
      if line['code'].to_i == 0
          if line['code'].to_s.empty?
              unless line['description'].nil?
                  @TV_codebook[index-1]['description'] +=  " #{line['description']}"
              end
              #puts "previous line codebook [index: #{index}] : #{codebook[index-1]["description"]} "
          else
              if line['code'].to_s != "*"
                  toplevel = line['code'].to_s
              end
          end
          #puts "codebook [#{index}] : #{codebook[index-1]["description"]} "
      end
      line['toplevel'] = toplevel
  }
  @TV_codebook =  @TV_codebook.reject { |line| line['code'].to_i == 0 }

  @logger.info ("READ #{SOURCE_DIR}/#{icandid_config.config[:IPTCMediaTopic]}")
  @IPTC_codebook =  collector::csv_file_to_hash("#{SOURCE_DIR}/#{icandid_config.config[:IPTCMediaTopic]}", ";")

  @logger.info ("Start parsing using config: #{ icandid_config.config.path}/#{ icandid_config.config.file} ")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  
  @logger.info ("Parsing for periodes in : #{ icandid_config.query_config.path }#{ icandid_config.query_config.file }")

  rule_set =  icandid_config.config[:rule_set].constantize unless  icandid_config.config[:rule_set].nil?
  
  # recent_search records are downloaded to {{query_name}}/{{date}}/" 
  # - query_name is tanslitarted from query[:query][:name]
  # - date is download day (today) %Y_%m/%d
  options = {
    :type => "VideoObject",
    :publisher => {
      :vtm => { 
          :@type => "Organization",
          :@id => "iCANDID_ORGANIZATION_VTM",
          :name => "VTM"
      },
      :vrt => { 
          :@type => "Organization",
          :@id => "iCANDID_ORGANIZATION_VRT",
          :name => "VRT"
      }
    },
    :tv_codebook =>  @TV_codebook
  }

  source_records_dir = icandid_config.get_source_records_dir( options: options)
  source_file_name_pattern = icandid_config.get_file_name_pattern()

  icandid_config.query_config[:queries].each.with_index() do |periode, index|
    if periode[:completed]
      next
    end


    unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
      @logger.info ("NExt next")
      next
    end
    @logger.info ("Paring records for periode: #{ periode[:query]  } ")

    p = periode[:query][:id] 
  
    # options[:actoren] = collector.csv_file_to_hash("#{source_records_dir}/#{p}_actoren.csv")
    options[:prefixid]  = "#{INGEST_CONF[:prefixid]}_#{INGEST_CONF[:provider][:@id] }"


    @logger.info ("READ #{source_records_dir}/#{p}_actoren.csv")

    json = {}

    actoren = []
    actoren_csv = collector.csv_file_to_hash("#{source_records_dir}/#{p}_actoren.csv")
    # actoren_csv =  actoren_csv[0..10]
    
    actoren_csv.each.with_index() do |record, index| 
      rec = {}
      record.to_h.keys.each do |k|
        rec[k] = record[k]
      end
      actoren << rec
    end

    @logger.info ("READ #{source_records_dir}/#{p}_thema.csv")
    thema = []
    thema_csv = collector.csv_file_to_hash("#{source_records_dir}/#{p}_thema.csv")

    puts "------------- > #{thema_csv.size}"
    # thema_csv =  thema_csv[11500..]
    
    thema_csv.each.with_index() do |record, index| 
      rec = {}
      record.to_h.keys.each do |k|
        rec[k] = record[k]
      end
      thema << rec
    end
    
    source_file = "#{source_records_dir}/temp.json"
    json[:actoren] = actoren

    thema.each_slice(1000).to_a.each do |t|
      json[:thema] = t
      File.open(source_file,"w") do |f|
        f.write(json.to_json )
      end
      
      @logger.info ("READ chunk of 1000 records from {source_records_dir}/#{p}_thema.csv")

      collector.parse_data( file: source_file, options: options, rule_set: rule_set )

      dir_options = { 
        :query =>  { 
          :id => p,
          :name => "#{p}"
        },
        :date => Date.today.strftime("%Y/%m/%d")
      }
      collector.write_records( records_dir:  icandid_config.get_records_dir( options:dir_options) ) 

    end

    periode[:query][:completed] = true

    icandid_config::update_query_config(query: periode, index: index)

  end

  icandid_config.update_system_status("ready")

rescue StandardError => e
  @logger.error("#{ e.message  }")
  @logger.error("#{ e.backtrace.inspect   }")

  importance = "High"
  subject = "[ERROR] iCANDID ENA parsing"
  message = <<END_OF_MESSAGE
  
  <h2>Error while parsing ENA data</h2>
  <p>#{e.message}</p>
  <p>#{e.backtrace.inspect}</p>
  
  <hr>
  
END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

  @logger.info("ENA Parsing is finished with errors")
ensure

  importance = "Normal"
  subject = "iCANDID #{INGEST_CONF[:provider][:name]} parsing"
  message = <<END_OF_MESSAGE

  <h2>Parsing #{INGEST_CONF[:provider][:name]} [#{INGEST_CONF[:provider][:@id]}] data</h2>
  Parsing using config: #{ icandid_config.config.path}/#{ icandid_config.config.file}
<H3>#{$0} </h3>
command_line_options :<br/> #{ icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }

  <hr>

END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

  @logger.info("Twitter Parsing is finished without errors")

end


