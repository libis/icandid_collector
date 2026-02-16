#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')


# Reddit 
# https://api.pullpush.io/reddit/search/submission?subreddit=StopGaming&before=1735689600


require 'icandid_collector'
provider = 'reddit'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG


def process_queries(icandid_config, options: {})
    begin

        input_directory="/source_records/Reddit/reddit_query_0000001/2025/**/"
        @logger.info ("Start processing files from #{input_directory}")

        icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
        
        if icandid_config.config[:rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = icandid_config.config[:rule_set].constantize 
        end


        files = icandid_input.select_files_from_source_records_dir(source_records_dir: input_directory, source_file_name_pattern: "test.json",  last_parsing_datetime: "2020-01-01" )
        
        input_options = {}

        suffix = "_with_comments"

        files.each do | filename |
            pp filename
            url = "file://#{filename}"

            # Split the filename into base and extension
            dir  = File.dirname(filename)
            basename = File.basename(filename, File.extname(filename))
            extension = File.extname(filename)

            # Append the suffix before the extension
            new_filename = "#{basename}#{suffix}#{extension}"


            icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
            data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
            
            data = data["data"] unless data["data"].is_a?(Array) # Correction dor wrong download {"data":{"data":[]}}
            # results with no name-property have probably been deleted
            data["data"].select! { |d| d["id"] }

            pp "numer of records #{data["data"].size}"

#            icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
 #           icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

#            options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"


            output = DataCollector::Output.new
            rules_ng.run( rule_set[:rs_expand_with_comments], data["data"], output, options )
    
            pp "#{output["expand_with_comments"].size}"

            icandid_output = IcandidCollector::Output.new( data: output["expand_with_comments"], icandid_config: icandid_config)
            icandid_output.save_data_to_uri( uri: "file://#{ File.join(dir, new_filename) }" , options: {"content_type": "application/json"})

        end
    end
end

begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }

    icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    
    @logger.info ("Start downloading using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Download for queries in : #{File.join( icandid_config.query_config.path , icandid_config.query_config.name) }")

    process_queries(icandid_config)

rescue => exception
    @logger.error("Error : #{ exception } ")
ensure
    puts "Todo : send mail ?"
end






