#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
require "unicode"
require 'logger'
require 'icandid'


include Icandid

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG
@total_nr_enriched_records = 0

ADMIN_MAIL_ADDRESS = "tom.vanmechelen@kuleuven.be"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

Dir[  File.join( ROOT_PATH,"src/rules/rosetta_*.rb") ].each {|file| require file;  }
Dir[  File.join( ROOT_PATH,"src/rules/*googe_ai*.rb") ].each {|file| require file;  }

#SOURCE_DIR   = '/source_records/googleapi/results/fotoalbums/**/'
SOURCE_DIR   = '/source_records/googleapi/results/fotoalbums/IE13361000/**/'

SOURCE_FILE_NAME_PATTERN = "*.json"

PROCESS_TYPE  = "enrichment"  # used to determine (command line) config options

ConfJson = File.read(File.join(ROOT_PATH, './config/config.cfg'))
ICANDID_CONF = JSON.parse(ConfJson, :symbolize_names => true)

ingestConfJson =  File.read(File.join(ROOT_PATH, './config/rosetta/ingest.cfg'))
INGEST_CONF = JSON.parse(ingestConfJson, :symbolize_names => true)

es_file = File.join( ROOT_PATH,"./config/elastic/config.yml")
es_config = YAML::load_file(es_file)

#INGEST_CONF[:prefixid] = ICANDID_CONF[:prefixid]

#INGEST_CONF[:url_prefix] = ICANDID_CONF[:url_prefix]

#INGEST_CONF[:genericRecordDesc] = "Entry from #{INGEST_CONF[:dataset][:name]}"

#############################################################
#TESTING = true
STATUS = "parsing"

begin
 

  config = {
    :config_path => File.join(ROOT_PATH, './config/rosetta/'),
    :config_file => "config.yml",
    :query_config_path => File.join(ROOT_PATH, './config/rosetta/'),
    :query_config_file => "queries.yml"
  }

  icandid_config = Icandid::Config.new( config: config )
  
  collector = IcandidCollector::Input.new( icandid_config.config ) 
  es_collector = IcandidCollector::Input.new( icandid_config.config ) 

  loader = Elastic.new()

  loader.es_version = es_config[:es_version]
  loader.es_url = "https://#{es_config[:user]}:#{es_config[:password]}@#{es_config[:base_url]}"
  loader.logger = @logger
  loader.check_elastic()


  source_records_dir = SOURCE_DIR
  source_file_name_pattern = SOURCE_FILE_NAME_PATTERN
  rule_set = RULE_SET_GOOGLE_IA_1_v1_0


  Dir["#{source_records_dir}/#{source_file_name_pattern}"][0..10].each_with_index do |source_file, index| 
    options = { :prefixid => "#{INGEST_CONF[:prefixid]}_#{ INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }" }

    collector.parse_data( file: source_file, options: options, rule_set: rule_set )
    google_ai_result = collector.output.data[:google_ai_result]

    s = source_file.split('/')

    id = "iCANDID_kadoc_fotoalbums_query_0000001_#{s[5]}_#{ s[7].split('_')[0] }"

    es_data = loader.get_document_by_id( index: es_config[:index] , id: id )


    pp "WHAT IF ES_DATA IS NIL ?"

    #pp "============ ES_DATA ==========="
    #pp es_data
    #pp "============ GOOGLE AI  ==========="
    #pp google_ai_result

    es_data = es_data.deep_merge( google_ai_result )

    #pp "============ ES_DATA ==========="
    #pp es_data
    #pp "--------------------"
    #pp es_data["@id"]

    pp "TYPE IS is index! shouldn't it be update ??? what with pipeline"
    jsondata = [ { "index": {
      "_index": es_config[:index],
      "pipeline": es_config[:pipeline],
      "_id": id
      }},
      es_data
    ]

    rest =  loader.load_to_es(jsondata: jsondata, es_client: @es_client,logger:  @logger)
    pp rest
  
  end

rescue StandardError => e
  @logger.error("#{ e.message  }")
  @logger.error("#{ e.backtrace.inspect   }")

  importance = "High"
  subject = "[ERROR] iCANDID #{INGEST_CONF[:provider][:name]} enriching 1"
  message = <<END_OF_MESSAGE

  <h2>Error while enriching #{INGEST_CONF[:provider][:name]} data</h2>
  <p>source_file #{source_file}</p>
  <p>#{e.message}</p>
  <p>#{e.backtrace.inspect}</p>
  
  <hr>
  
END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)

  @logger.info("#{INGEST_CONF[:provider][:name]} Enriching is finished with errors")
ensure

  importance = "Normal"
  subject = "iCANDID #{INGEST_CONF[:provider][:name]} Enriching [#{@total_nr_enriched_records}]"
  message = <<END_OF_MESSAGE

  <h2>Enriching #{INGEST_CONF[:provider][:name]} [#{INGEST_CONF[:provider][:@id]}] data</h2>


  <hr>

END_OF_MESSAGE

  Icandid::Utils.mailErrorReport(subject, message, importance, config)
  @logger.info("#{INGEST_CONF[:provider][:name]} Enriching is finished without errors")

end
