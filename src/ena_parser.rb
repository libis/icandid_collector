#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

require 'icandid_collector'
provider = 'ENA'

PROCESS_TYPE = "parser"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

def parse_queries(options: {})
    begin
        if @icandid_config.config[:rule_set].nil?
            raise "rule_set is required to parse file"
        else
            rule_set = @icandid_config.config[:rule_set].constantize 
        end

        options[:type] = "VideoObject"
        options[:tv_codebook] = @TV_codebook
        options[:publisher] = {
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
        }

        @icandid_config.queries_to_process.each do |query|
            @icandid_config.config[:query] = query

            if query[:query][:completed]
              next
            end

            @icandid_config.update_config_with_query_data( query: query, options: options )    

            @logger.info ("Parse records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_input  = IcandidCollector::Input.new( :icandid_config => @icandid_config)#
           
            @logger.info ("Start parsing query: #{ query[:query][:name] } ")
            @logger.info ("Start parsing source_records_dir: #{@icandid_config.config[:source_records_dir]} ")           
            # @logger.info ("Start parsing source_file_name_pattern: #{@icandid_config.config[:source_file_name_pattern]} ")

            period = query[:query][:id]
            @logger.info ("Start parsing period: #{period} ")

            # Load CSV data
            base_dir = @icandid_config.config[:source_records_base_dir]
            actoren_csv = @icandid_utils.csv_file_to_hash(File.join(base_dir, "#{period}_actoren.csv"))
            thema_csv   = @icandid_utils.csv_file_to_hash(File.join(base_dir, "#{period}_thema.csv"))

            # Build JSON structure
            json_output = {
              actoren: actoren_csv.map(&:to_h),
              thema:   thema_csv.map(&:to_h)
            }

            # Write thema in chunks of 1000
            temp_dir = File.join( base_dir, "temp")
            
            # Ensure output directory exists
            FileUtils.mkdir_p(temp_dir)


            json_output[:thema].each_slice(1000).with_index do |chunk, counter|
              output_path = File.join(temp_dir, "#{period}_#{counter}.json")
              File.open(output_path, "w") do |f|
                f.write({ actoren: json_output[:actoren], thema: chunk }.to_json)
              end
              @logger.info("Wrote chunk ##{counter} with 1000 records to #{output_path}")
            end

            #thema_csv.each.with_index() do |record, indexexi| 
            #  rec = {}
            #  record.to_h.keys.each do |k|
            #    rec[k] = record[k]
            #  end
            #  thema << rec
            #end

            @icandid_config.config[:source_records_dir] = temp_dir
            icandid_input.process_files( options: options  )
            @icandid_config.config[:source_records_dir] = base_dir

            

            @logger.info ("Start parsing next NEXT NEXT ")

        end
    end
end


begin

  @logger = Logger.new(STDOUT)
  @logger.level = Logger::DEBUG
  @total_nr_parsed_records = 0  

  start_processing =  Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")

  # Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| require file; }

  config = {
      :config_path => File.join(ROOT_PATH, "./config/#{provider}")
  }

  @icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA)
  @icandid_input  = IcandidCollector::Input.new( :icandid_config => @icandid_config.config ) 
  @icandid_utils  = IcandidCollector::Utils.new( :icandid_config => @icandid_config.config )
  
  @logger.info ("Start parsing using config: #{ File.join( config[:config_path] , "config.yml") }")

  @TV_codebook =  @icandid_utils.csv_file_to_hash("#{ @icandid_config.config[:source_records_base_dir] }/#{@icandid_config.config[:themavariabelen]}", ";")
  toplevel = ''

  # TV_codebook  csv-structure
  # <Toplevel>
  # <code>,<title>,<description>
  # <code>,<title>,<description>
  # ...


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

  @logger.info ("READ #{@icandid_config.config[:source_records_base_dir]}/#{@icandid_config.config[:IPTCMediaTopic]}")
  @IPTC_codebook =  @icandid_utils.csv_file_to_hash("#{@icandid_config.config[:source_records_base_dir]}/#{@icandid_config.config[:IPTCMediaTopic]}", ";")

  start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
  @logger.info ("Parsing for queries in : #{File.join( @icandid_config.query_config.path , "config.yml") }")

  options = {
    prefixid: "#{@icandid_config.ingest_data[:prefixid]}_#{ @icandid_config.ingest_data[:provider][:@id].downcase }_#{ @icandid_config.ingest_data[:dataset][:@id].downcase }",
    ingest_data: @icandid_config.ingest_data
  }


  # split to avoid looking for new files in the backlog directories
  parse_queries(options: options)

#    parse_backlog_queries(options: options)
  
  @icandid_config.queries_to_process.map! do |query|
      query[:last_parsing_datetime] = start_processing
      query
  end
  @icandid_config.update_query_config






=begin
  rule_set =  @icandid_config.config[:rule_set].constantize unless  @icandid_config.config[:rule_set].nil?
  
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

  source_records_dir = @icandid_config.get_source_records_dir( options: options)
  source_file_name_pattern = @icandid_config.get_file_name_pattern()

  @icandid_config.queries_to_process.map! do |query|
    query
  end



  icandid_config.query_config[:queries].each.with_index() do |period, index|
    if period[:completed]
      next
    end


    unless icandid_config.get_queries_to_parse.include?(query[:query][:id])
      @logger.info ("NExt next")
      next
    end
    @logger.info ("Paring records for period: #{ period[:query]  } ")

    p = period[:query][:id] 
  
    options[:prefixid]  = "#{INGEST_CONF[:prefixid]}_#{INGEST_CONF[:provider][:@id] }"


    @logger.info ("READ #{source_records_dir}/#{p}_actoren.csv")

    json = {}

    actoren = []
    actoren_csv = @icandid_utils.csv_file_to_hash("#{source_records_dir}/#{p}_actoren.csv")
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
    thema_csv = @icandid_utils.csv_file_to_hash("#{source_records_dir}/#{p}_thema.csv")

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
      collector.write_records( records_dir:  @icandid_config.get_records_dir( options:dir_options) ) 

    end

    period[:query][:completed] = true

    icandid_config::update_query_config(query: period, index: index)

  end

  icandid_config.update_query_config
=end




    
rescue StandardError => e
    @logger.error("#{ e.message  }")
    @logger.error("#{ e.backtrace.inspect   }")
  
    importance = "High"
    subject = "[ERROR] iCANDID #{@icandid_config.ingest_data[:provider][:name]} parsing"
    message = <<END_OF_MESSAGE
    
    <h2>Error while parsing #{@icandid_config.ingest_data[:provider][:name]} data</h2>
    <p>#{e.message}</p>
    <p>#{e.backtrace.inspect}</p>
    
    <hr>
    
END_OF_MESSAGE
  
    @icandid_utils.mailErrorReport(subject, message, importance, @icandid_config) 
    @logger.info("#{@icandid_config.ingest_data[:provider][:name]} Parsing is finished with errors")

ensure
  
    importance = "Normal"
    subject = "iCANDID #{@icandid_config.ingest_data[:provider][:name]} parsing [#{@total_nr_parsed_files} => #{@icandid_config.config[:nbr_created_records]}]"
    message = <<END_OF_MESSAGE
    
    <h2>Parsing #{@icandid_config.ingest_data[:provider][:name]} [#{@icandid_config.ingest_data[:provider][:@id]}] data</h2>
    Parsing using config: : #{File.join( @icandid_config.query_config.path , "config.yml") }"
  <H3>#{$0} </h3>
  command_line_options :<br/> #{ @icandid_config.command_line_options.map { |k, v|  "  - #{k}: #{v} </br>" }.join   }
    <br/>
    total_nr_parsed_files : #{@total_nr_parsed_files}
    <br/>
    nbr_created_records : #{@icandid_config.config[:nbr_created_records]}

  
    <hr>
  
END_OF_MESSAGE

    @icandid_utils.mailErrorReport(subject, message, importance, @icandid_config)
    # @logger.info("#{icandid_config.ingest_data[:provider][:name]} Parsing is finished without errors")
  
end