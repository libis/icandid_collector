#encoding: UTF-8
# Samenstellen van de lijst met scopeArchive id's:
# - in de directory met rosetta-export-xml bestanden 
#        grep 'dc:identifier xsi:type="dcterms:URI">http://abs.lias.be/Query/detail.aspx?ID=' SET1/*.xml | cut -d "=" -f 3 | cut -d "<" -f 1 > scopeArchiv_ids
#
# Op basis van een lijst met scopeArchiv id's (bestand scopeArchiv_ids ) worden de records gedownload via een call naar Oracle
# conn.exec("BEGIN KUL_PACKAGES.scope_xml_meta_data_by_id(#{scope_id}); END;")
# 


$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'logger'
require 'oci8'


require 'icandid_collector'
provider = 'scopeArchiv'

PROCESS_TYPE = "download"

# require 'dbi'

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider}_*.rb") ].each {|file| require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}")
    }

    icandid_config = IcandidCollector::Configs.new( :config => config ) 

    conn = OCI8.new(icandid_config.config[:oracle][:username], icandid_config.config[:oracle][:password], icandid_config.config[:oracle][:dbname])

    @logger.info ("Export records from #{icandid_config.config[:oracle][:dbname]} based on list of scope_ids")

    icandid_config.queries_to_process.map!.with_index() do |query, index|
        
        file_name  = "/source_records/#{provider}/#{query[:query][:id]}/scopeArchiv_ids"
        @logger.info ("read file with scope ids #{file_name}")

        scope_ids = File.readlines(file_name).map(&:chomp)
        options = {}
        icandid_config.config[:query] = query
        icandid_config.update_config_with_query_data( query: query, options: options )

        @logger.info ("Query : #{query}")

        scope_ids = [ "IE2489457", "IE12969158", "IE13361347", "IE2461426"]

        scope_ids.each do |scope_id|


pp "------------------------ #{scope_id} ---------------"
            cursor = conn.parse ('begin :ret := KUL_PACKAGES.scope_xml_meta_data_by_pid(:MY_ID); end;')
            cursor.bind_param(':MY_ID', scope_id, String)
            cursor.bind_param(':ret', OCI8::CLOB)
            cursor.exec() 

pp cursor[':ret'].read 

=begin
            @logger.debug ("scope_id : #{scope_id}")

            @logger.info ("save file to source_records_dir : #{icandid_config.config[:source_records_dir]}")

            file_name = File.join(icandid_config.config[:source_records_dir] , "#{scope_id}.xml")

            file_name_absolute_path = File.absolute_path(file_name)
            file_directory = File.dirname(file_name_absolute_path)
    
            unless File.directory?(file_directory)
              FileUtils.mkdir_p(file_directory)
            end
    
            File.open(file_name_absolute_path, 'wb:UTF-8') { |file| file.write( cursor[':ret'].read ) }
=end            
            #@logger.info (" Records will be available in /nas/vol03/oracle/SCOPEP/ ")
            #@logger.info ("create oracle export for #{scope_id} with KUL_PACKAGES.scope_xml_meta_file_by_id")
            #pp "BEGIN KUL_PACKAGES.scope_xml_meta_data_by_id(#{scope_id}); END;"
            #conn.exec("BEGIN KUL_PACKAGES.scope_xml_meta_file_by_id(#{scope_id}); END;")

        end
        
    end

end
