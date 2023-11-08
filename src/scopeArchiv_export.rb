#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'logger'
require 'oci8'

require 'icandid_collector'

PROCESS_TYPE = "download"

# require 'dbi'


begin

    scope_ids = File.readlines('/source_records/scopeArchiv/fotoalbums_query_0000001/scopeArchiv_ids').map(&:chomp)

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, './config/scopeArchiv')
    }

    icandid_config = IcandidCollector::Configs.new( :config => config ) 

    conn = OCI8.new(icandid_config.config[:oracle][:username], icandid_config.config[:oracle][:password] ,icandid_config.config[:oracle][:dbname])

    @logger.info (" Records will be available in /nas/vol03/oracle/SCOPEP/ ")

    scope_ids.each do |scope_id|
        @logger.info ("create oracle export for #{scope_id} with KUL_PACKAGES.scope_xml_meta_file_by_id")
        conn.exec("BEGIN KUL_PACKAGES.scope_xml_meta_file_by_id(#{scope_id}); END;")
    end

end
