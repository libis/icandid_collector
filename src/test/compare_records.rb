#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../../')

require 'http'
require 'open-uri'
require 'tmpdir'
require 'jsonpath'
require 'json'
require 'open3'

require_relative "./helpers/diff"
require_relative "./helpers/normalize"
require_relative "./helpers/record"


test_files_directory = "/app/src/test/features/vlaamsparlement/"
output_dir =  "/app/src/test/features/vlaamsparlement/temp20251128-26787-k2e3lj/"

records_dir = File.join(test_files_directory, "records")


additional_file_processing = Proc.new do |local_data, es_data|

  local_data_datePublished =  Date.parse(local_data["datePublished"])
  if es_data["datePublished"].is_a?(Array)
    es_data_datePublished = es_data["datePublished"].map { |d| Date.parse(d) }.max
  else
    es_data_datePublished =  Date.parse(es_data["datePublished"])
  end

  # creativeWorkStatus wijzigd in tijd
  # Een document van het vlaamsparment wordt geupdate met nieuwe data en oa nieuwe creativeWorkStatus
  if es_data_datePublished < local_data_datePublished
    es_data["creativeWorkStatus"] = local_data["creativeWorkStatus"]
    es_data["datePublished"] = local_data["datePublished"]
  end
  
  if local_data["@id"].match(/^iCANDID_vlaamsparlement_vlpar_opendata_query_0000001_[0-9]*-00000/i)
    if local_data["sameAs"] != es_data["sameAs"]
      processing_date = Date.parse(es_data["processingtime"])
      target_date = Date.new(2025, 9, 30)
      if processing_date < target_date
        # This ES data is incorrect
        msg = "Current ES data is depricated! [ #{ local_data["@id"] }] "
        local_data = es_data
      end
    end
  end
  # Return something if condition not met
  [local_data, es_data, msg]
end

msg = verify_ingestion_consistency(output_dir, records_dir, additional_file_processing)

pp msg