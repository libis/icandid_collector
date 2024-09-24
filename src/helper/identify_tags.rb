#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"

require 'icandid_collector'

ROOT_PATH = File.join( File.dirname(__FILE__), '../')

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG
@source_records_dir = "/source_records/scopeArchiv/EAD/kadoc_ead_query_0000001/"
@source_file_name_pattern = '.*.xml'
@last_parsing_datetime = "20000101"


class ::Hash
  def deep_merge(second)
      merger = proc { |key, v1, v2| Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : v2 }
      self.merge(second, &merger)
  end
end




def select_files_from_source_records_dir(source_records_dir: nil, source_file_name_pattern: nil,  last_parsing_datetime: nil )
  files = []

  Dir["#{source_records_dir}/*"].each do |source_file| 

    if File.directory?( source_file )
      files.concat select_files_from_source_records_dir( source_records_dir: source_file, source_file_name_pattern: source_file_name_pattern,  last_parsing_datetime: last_parsing_datetime )
    else
      if Regexp.new(source_file_name_pattern).match(source_file)
        files << source_file
      end
    end
  end
  files.uniq
end


def get_struct(input)
  output = {}

  case input
  when Hash
    input.each { |k,v|
      output[k] = get_struct( v )
    }
  when Array
    input.each { |e|
      output = output.deep_merge( get_struct(e) )
    }
  when String
#    pp "String"
  end
  output
end

begin

  icandid_input  = IcandidCollector::Input.new( )

  record_files = select_files_from_source_records_dir(
    source_records_dir:       @source_records_dir,
    source_file_name_pattern: @source_file_name_pattern,
    last_parsing_datetime:    @last_parsing_datetime
  )

  pp record_files
  structure = {}

  record_files.each do |source_file|
    output.clear()
    #input = DataCollector::Input.new
    #output = DataCollector::Output.new
    data = icandid_input.collect_data_from_uri(url: "file://#{ source_file }", options: {} )

    struct = get_struct ( data )
    structure= structure.deep_merge(struct)
    
  end
  pp "------------------------------------------------------------"
  pp structure
  pp "------------------------------------------------------------"
  

end