#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../../')

require 'http'
require 'open-uri'
require 'tmpdir'
require "minitest/autorun"
require 'jsonpath'
require 'json'
require 'open3'

require_relative "./helpers/diff"
# require_relative "./helpers/normalize"
require_relative "./helpers/record"

ES_USER     = ENV['ES_USER']
ES_PASSWORD = ENV['ES_PASSWORD']
ES_URL      = ENV['ES_URL']
ES_INDEX    = ENV['ES_INDEX']

REQUEST_OPTIONS = {
    user: ES_USER,
    password: ES_PASSWORD,
    #url: "#{ENV['ES_URL']}/icandid/_doc",
    url: "https://host.docker.internal:9200/icandid/_doc",
    method: "get",
    verify_ssl: true,
    headers: { "Content-Type" => "application/json" }
}






class DataCollectorTest < Minitest::Test

  def test_vlaamsparlement_parser
    puts ""
    pp "######################################################################"
    pp " Test parsing and rules from vlaams parlement                          "
    pp "######################################################################"

    test_files_directory = "/app/src/test/features/vlaamsparlement/"
    temp_directory = Dir.mktmpdir("temp", test_files_directory)
    source_records_dir = File.join(test_files_directory, "source_records")
    records_dir = File.join(test_files_directory, "records")

    pp "temp_directory: #{temp_directory}"

    parser_script = "/app/src/vlaamsparlement_parser.rb"
    config_file = "/app/config/VlaamsParlement/config.yml"
    query_config_file = "/app/config/VlaamsParlement/queries/config.yml"
    query_id = "vlpar_opendata_query_0000001"
    input_dir = source_records_dir
    output_dir = temp_directory
    file_pattern = "input_[\\d]*.json"
    # file_pattern = "test_input.json"
    # file_pattern = "input_9.json"
    start_time = "2020-01-01"

    # generate new records in a temporary output directory
    command = [
      "ruby", parser_script,
      "-c", config_file,
      "-q", query_config_file,
      "--query_id", query_id,
      "-s", input_dir,
      "-p", file_pattern,
      "-u", start_time,
      "-d", output_dir
    ]

    begin
      pp "Ruby command: #{command.join( " ")}"
      
      stdout, stderr, status = Open3.capture3(*command)
      STDERR.puts "STDOUT:\n#{stdout}"
      STDERR.puts "STDERR:\n#{stderr}"
      STDERR.puts "Exit status: #{status.exitstatus}"

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

      # Cross-check newly generated records with previously ingested records.
      msg = verify_ingestion_consistency(output_dir, records_dir, additional_file_processing)

      unless msg.nil? || msg.empty?
        flunk msg
      end
      
      assert status.success?, "Parser script failed: #{stderr}"
  
    ensure
      # Clean up temp directory
      if Dir.exist?(temp_directory)
        pp "Removing temp directory: #{temp_directory}"
        FileUtils.remove_entry(temp_directory)
      end
    end
  end

  def test_zweedsparlement_parser
    pp ""
    pp "######################################################################"
    pp " Test parsing and rules from zweed parlement                          "
    pp "######################################################################"

    test_files_directory = "/app/src/test/features/zweedsparlement/"
    temp_directory = Dir.mktmpdir("temp", test_files_directory)
    source_records_dir = File.join(test_files_directory, "source_records")
    records_dir = File.join(test_files_directory, "records")
      
    parser_script = "/app/src/zweedsparlement_parser.rb"
    config_file = "/app/config/ZweedsParlement/config.yml"
    query_config_file = "/app/config/ZweedsParlement/queries/config.yml"
    query_id = "data_riksdagen_query_0000001"
    input_dir = source_records_dir
    output_dir = temp_directory
    file_pattern = "input_[\\d]*.json"

    start_time = "2020-01-01"
    

    command = [
      "ruby", parser_script,
      "-c", config_file,
      "-q", query_config_file,
      "--query_id", query_id,
      "-s", input_dir,
      "-p", file_pattern,
      "-u", start_time,
      "-d", output_dir
    ]

    begin
      stdout, stderr, status = Open3.capture3(*command)
      STDERR.puts "STDOUT:\n#{stdout}"
      STDERR.puts "STDERR:\n#{stderr}"
      STDERR.puts "Exit status: #{status.exitstatus}"

      # Cross-check newly generated records with previously ingested records.
      msg = verify_ingestion_consistency(output_dir, records_dir)
      
      unless msg.nil? || msg.empty?
        flunk msg
      end
      
      assert status.success?, "Parser script failed: #{stderr}"
  
    ensure
      # Clean up temp directory
      if Dir.exist?(temp_directory)
        pp "Removing temp directory: #{temp_directory}"
        #FileUtils.remove_entry(temp_directory)
      end
    end
  end

  def test_ena_parser
    pp ""
    pp "######################################################################"
    pp " Test parsing and rules from ENA                                      "
    pp "######################################################################"

    test_files_directory = "/app/src/test/features/ENA/"
    temp_directory = Dir.mktmpdir("temp", test_files_directory)
    source_records_dir = File.join(test_files_directory, "source_records")
    records_dir = File.join(test_files_directory, "records")
      
    parser_script = "/app/src/ena_parser.rb"
    config_file = "/app/config/ENA/config.yml"
    query_config_file = "/app/config/ENA/queries/config.yml"
    query_id = "2023"
    input_dir = source_records_dir
    output_dir = temp_directory
    file_pattern = ".*"

    start_time = "2020-01-01"
    

    command = [
      "ruby", parser_script,
      "-c", config_file,
      "-q", query_config_file,
      "--query_id", query_id,
      "-s", input_dir,
      "-p", file_pattern,
      "-u", start_time,
      "-d", output_dir
    ]

    begin

      pp "Ruby command: #{command.join( " ")}" 

      stdout, stderr, status = Open3.capture3(*command)
      STDERR.puts "STDOUT:\n#{stdout}"
      STDERR.puts "STDERR:\n#{stderr}"
      STDERR.puts "Exit status: #{status.exitstatus}"

      # Cross-check newly generated records with previously ingested records.
      msg = verify_ingestion_consistency(output_dir, records_dir)
      
      unless msg.nil? || msg.empty?
        flunk msg
      end
      
      assert status.success?, "Parser script failed: #{stderr}"
  
    ensure
      # Clean up temp directory
      if Dir.exist?(temp_directory)
        pp "Removing temp directory: #{temp_directory}"
        # FileUtils.remove_entry(temp_directory)
      end
    end
  end

  def test_tmdb_parser
    pp ""
    pp "######################################################################"
    pp " Test parsing and rules from TMDB                                     "
    pp "######################################################################"

    test_files_directory = "/app/src/test/features/TMDB/"
    temp_directory = Dir.mktmpdir("temp", test_files_directory)
    source_records_dir = File.join(test_files_directory, "source_records")
    records_dir = File.join(test_files_directory, "records")
      
    parser_script = "/app/src/tmdb_parser.rb"
    config_file = "/app/config/TMDB/config.yml"
    query_config_file = "/app/config/TMDB/queries/config.yml"
    query_id = "tmdb_query_00001"
    input_dir = source_records_dir
    output_dir = temp_directory
    file_pattern = ".*.json"

    start_time = "2020-01-01"
    

    command = [
      "ruby", parser_script,
      "-c", config_file,
      "-q", query_config_file,
      "--query_id", query_id,
      "-s", input_dir,
      "-p", file_pattern,
      "-u", start_time,
      "-d", output_dir
    ]

    begin
      stdout, stderr, status = Open3.capture3(*command)
      STDERR.puts "STDOUT:\n#{stdout}"
      STDERR.puts "STDERR:\n#{stderr}"
      STDERR.puts "Exit status: #{status.exitstatus}"

      # Cross-check newly generated records with previously ingested records.
      msg = verify_ingestion_consistency(output_dir, records_dir)
      
      unless msg.nil? || msg.empty?
        flunk msg
      end
      
      assert status.success?, "Parser script failed: #{stderr}"
  
    ensure
      # Clean up temp directory
      if Dir.exist?(temp_directory)
        pp "Removing temp directory: #{temp_directory}"
        FileUtils.remove_entry(temp_directory)
      end
    end
  end

end