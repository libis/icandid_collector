#encoding: UTF-8
require "iso639"
require 'iso8601'
require "unicode"
require "unicode/scripts"
require 'optparse'
require 'net/smtp'
require 'rubygems'
require 'rubygems/package'
require 'zlib'
require 'fileutils'
require 'data_collector'


require 'icandid_collector'

include DataCollector::Core

# =========== TODO ===========================================
# - add enabled/disable functionality trough query.yml
# - make the retry waiting period configurable per provider
#
# ############################################################


class ::Hash
  def deep_merge(second)
    merger = proc { |_, v1, v2| 
      #Hash === v1 && Hash === v2 ? v1.merge(v2, &merger) : Array === v1 && Array === v2 ? v1 | v2 : [:undefined, nil, :nil].include?(v2) ? v1 : v2 
      if Hash === v1 && Hash === v2 
        v1.merge(v2, &merger) 
      else
        if Array === v1 && Array === v2 
          v = v1 | v2 
          v = v.reduce([]) do | array, hash|
            if hash["@id"].nil?
              array << hash
            else
              if array.map { |m| m["@id"] }.include?( hash["@id"] )
                array.map! { |m| 
                  if m["@id"] == hash["@id"] 
                    m.merge(hash, &merger)
                  else
                    m
                  end
                }
              else
                array << hash
              end
            end
          end
          v
        else
          if [:undefined, nil, :nil].include?(v2) 
            v1 
          else
            v2 
          end
        end
      end
    }
    merge(second.to_h, &merger)
  end
end

module Icandid
  class Config

    attr_accessor :config, :query_config

    def initialize( config: {})
      @logger  = Logger.new(STDOUT)

      @command_line_options = {}

      @config = DataCollector::ConfigFile.clone
      @config.path = config[:config_path]
      @config.file = config[:config_file]
      
      @query_config  = DataCollector::ConfigFile.clone
      @query_config.path = config[:query_config_path]
      @query_config.file = config[:query_config_file]


     # puts "---- Icandid::Config.initialize config ---" 
     # pp @config
     # pp @config.methods

      update_config_with_command_line_options()
      check_system_status()
    end

    def get_command_line_options()

      if PROCESS_TYPE == "count"
        option_parser = OptionParser.new do |o|
          o.banner = "Usage: #{$0} [options]"
          o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
          o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
          o.on("-n QUERY_ID", "--query_id QUERY_ID", "=MANDATORY", "The id of one specific query that needs to be counted") { |n| @command_line_options[:query_id] = n}
          o.on( '-h', '--help', 'Display this screen.' ) do
            puts o
            exit
          end
          o.parse!   
        end
        if  @command_line_options[:query_id].nil?
          puts option_parser.help
          exit 1
        end
      end

      if PROCESS_TYPE == "download"
        option_parser = OptionParser.new do |o|
          o.banner = "Usage: #{$0} [options]"
          o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
          o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
          o.on("-n QUERY_ID", "--query_id", "The id of the query if only one specific query needs to be downloaded") { |n| @command_line_options[:query_id] = n}
          o.on("-s SOURCE_RECORDS_DIR", "--source", "Directory where the records will be stored (/source_records/<provider>/{{query}}/)") { |s| @command_line_options[:source_dir] = s}
          o.on( '-h', '--help', 'Display this screen.' ) do
            puts o
            exit
          end
          o.parse!   
        end
      end

      if PROCESS_TYPE == "parser"
        option_parser = OptionParser.new do |o|
            o.banner = "Usage: #{$0} [options]"
          # o.on("-l LOGFILE", "--log", "write log to file") { |log_file| @log_file = log_file}
            o.on("-c CONFIG", "--config", "yml-file (including path) with the configuration (./config/config.yml)") { |c| @command_line_options[:config] = c }
            o.on("-i INGEST_FILE", "--ingest", "Ingest config file") { |i| ingest_file = i;  @command_line_options[:ingest_file] = i}
            o.on("-q QUERY_FILE", "--query", "yml-file (including path) with query configuration (url-parameters, last_update_datetime)  (./config/queries.yml)") { |q| @command_line_options[:query_config] = q}
            o.on("-n QUERY_ID", "--query_id", "The id of the query if only one specific query needs to be parsed") { |n| @command_line_options[:query_id] = n}
            o.on("-s SOURCE_RECORDS_DIR", "--source", "Directory of the source records (/source_records/<provider>/{{query}}/") { |s| @command_line_options[:source_dir] = s}
            o.on("-d DESTINATION_RECORDS_DIR", "--destination", "Directory to save the parsend schema.org json-ld records (/records/<provider>/{{query_id}})") { |d| @command_line_options[:dest_dir] = d}
            o.on("-p FILE_PATTERN", "--pattern", "file pattern of record-filename /<provider>*\\\.json/") { |p| @command_line_options[:source_file_name_pattern] = p}
            o.on("-u LAST_RUN", "--last_parsing_datetime", "Time the command was last run. Load files with modification time > LAST_RUN ") { |u| @command_line_options[:last_parsing_datetime] = u }    
            o.on("-b BASED_ON_DATE_PUBLISHED", "--dir_based_on_datePublished", "Make subfolders in destination based on DatePublished in the record (true/false)") { |b| @command_line_options[:dir_based_on_datePublished] = b }    
            o.on( '-h', '--help', 'Display this screen.' ) do
              puts o
              exit
            end
          o.parse!   
        end 
      end

    end

    def update_query_config ( query: , index: )
      if @query_config.nil?
        raise ("@query_config nil in update_query_config")
      end
      if query.nil?
        raise ("query missing in update_query_config")
      end
      query_config[:queries][index] = query
      query_config[:queries] = query_config[:queries]
    end

    def command_line_options
      @command_line_options
    end

    def update_config_with_command_line_options()
      @command_line_options = {}
      get_command_line_options
      unless @command_line_options[:config].nil?
        if File.exist?(@command_line_options[:config])
            @config.path = File.dirname(@command_line_options[:config])
            @config.file  = File.basename(@command_line_options[:config])
        else
            raise ("config #{@command_line_options[:config]} does not exist")
        end
      end

      unless @command_line_options[:query_config].nil?
          if File.exist?(@command_line_options[:query_config])
            @query_config.path = File.dirname(@command_line_options[:query_config])
            @query_config.file = File.basename(@command_line_options[:query_config])
          else
            raise ("config #{@command_line_options[:query_config]} does not exist")
          end
      end
    end

    def check_system_status
      unless STATUS == "parsing" && ! @command_line_options[:last_parsing_datetime].nil?
        if ! ["ready",STATUS].include?(  @config[:status] )
          message = "Unable to start #{STATUS}. system is not ready: STATUS = #{  @config[:status] }"
          @logger.warn ("Unable to start #{STATUS}. system is not ready: STATUS = #{  @config[:status] }")
          @logger.info ("status in config_file (config.yml) should be ready or #{STATUS}")
          @logger.info ("Config files will be overridden if another process is still running")
          exit
        else
          @logger.debug ("update config_file (#{@config.file} ) with #{STATUS}")
          update_system_status "#{STATUS}"
        end
      end
    end

    def update_system_status( status )
      @config[:status] = status
    end

    def get_queries_to_parse( )
      queries_to_parse = queries_to_parse = query_config[:queries].map { |q|  q[:query][:id] }
      unless @command_line_options[:query_id].nil?
        queries_to_parse.select! { |q| q == @command_line_options[:query_id] }
        if queries_to_parse.empty?
          raise ("#{@command_line_options[:query_id]} does not exist in #{ query_config.path }#{ query_config.file }")
        end
      end
      queries_to_parse
    end

    def get_parsing_datetime( query:{} )
      # puts  query[:last_parsing_datetime] 

      # puts "get_parsing_datetime"      
      # puts       @command_line_options[:last_parsing_datetime]
      return Time.parse( @command_line_options[:last_parsing_datetime] ) unless  @command_line_options[:last_parsing_datetime].nil?
      return query[:last_parsing_datetime]  unless query[:last_parsing_datetime] .nil? ||query[:last_parsing_datetime].empty?
      return Time.parse("2000/01/01")
    end
    
    def get_file_name_pattern()
      @command_line_options[:source_file_name_pattern] || config[:source_file_name_pattern] || SOURCE_FILE_NAME_PATTERN
    end

    def get_source_records_dir( options: {} )
      source_records_dir = @command_line_options[:source_dir] || config[:source_records_dir] 
      if source_records_dir.nil?
        raise ("source_records_dir missing. add it to config or as -s on commandline")
      end

      options[:query_name] = I18n.transliterate( options[:query][:name] ).delete(' ').delete('\'') unless options[:query].nil?
      options[:query_id]   = options[:query][:id]  unless options[:query].nil?

      if options[:date].nil?
        if options[:collection_type] == "recent_records"
          options[:date]       = Time.now.strftime("%Y_%m/%d")  
        end
        if options[:collection_type] == "backlog"
          options[:date] = "#{Time.now.strftime("%Y-%m-%d")}/backlog/#{  options[:backlog][:current_process_date].to_datetime.strftime("%Y_%m") }/"
        end
      end

      source_records_dir = source_records_dir.gsub(/\{\{today\}\}/, Time.now.strftime("%Y/%m/%d"))
      source_records_dir = source_records_dir.gsub(/\{\{year\}\}/, Time.now.strftime("%Y"))
      source_records_dir = source_records_dir.gsub(/\{\{month\}\}/, Time.now.strftime("%m"))
      source_records_dir = source_records_dir.gsub(/\{\{day\}\}/, Time.now.strftime("%d"))
      source_records_dir = source_records_dir.gsub(/\{\{hour\}\}/, Time.now.strftime("%H"))


      source_records_dir.scan(/\{\{([^{}]*)\}\}/).each { |substitution|
        substitution = substitution[0]
        if  options[substitution.to_sym].nil?
          raise ("Missing option \"#{ substitution }\" to substitute source_records_dir")
        else
          source_records_dir = source_records_dir.gsub(/\{\{#{substitution}\}\}/, options[substitution.to_sym])         
        end
        
      }

      return source_records_dir
    end

    def get_records_dir( options: {} )

      records_dir =  @command_line_options[:dest_dir]  || config[:records_dir] 
      if records_dir.nil?
        raise ("records_dir missing. add it to config or as -s on commandline")
      end

      options[:query_name] = I18n.transliterate( options[:query][:name] ).delete(' ').delete('\'') unless options[:query].nil?
      options[:query_id]   = options[:query][:id]  unless options[:query].nil?

      # records_dir = records_dir.gsub(/\{\{month\}\}/, Time.now.strftime("%Y/%m/%d"))
      records_dir = records_dir.gsub(/\{\{year\}\}/, Time.now.strftime("%Y"))
      records_dir = records_dir.gsub(/\{\{month\}\}/, Time.now.strftime("%m"))
      records_dir = records_dir.gsub(/\{\{day\}\}/, Time.now.strftime("%d"))
      records_dir = records_dir.gsub(/\{\{hour\}\}/, Time.now.strftime("%H"))

      records_dir.scan(/\{\{([^{}]*)\}\}/).each { |substitution|
        substitution = substitution[0]
        if  options[substitution.to_sym].nil?
          raise ("Missing option \"#{ substitution }\" to substitute records_dir")
        else
          records_dir = records_dir.gsub(/\{\{#{substitution}\}\}/, options[substitution.to_sym])
          
        end
      }
      return records_dir
    end

    def clear_records_dir(records_dir)
      @logger.info("DELETE RECORDS FROM #{ records_dir }*")
      puts "DELETE RECORDS FROM #{records_dir }* ???? "
      ## Dir.glob("#{records_dir}*").each { |file| File.delete(file) }
    end


    def create_recent_url( url: nil, query:{}, options: {} )
      # if query[:recent_records][:url] contains a value. This is where the previous process ended
      if url.nil?
        raise ("missing url for create_recent_url")
      end
      if query[:recent_records][:url] =~ URI::regexp
        @logger.warn "Continue were the previous unfinished process ended"
        url = query[:recent_records][:url]
      else
        url = create_url( url: url, query: query, options: options)
      end
      url
    end

    def create_record_url( url: nil, query:{}, options: {} )
      # if query[:backlog][:url] contains a value. This is where the previous process ended
      if url.nil?
        raise ("missing url for create_record_url")
      end
      create_url( url: url, query: query, options: options)
    end

    def create_backlog_url( url: nil, query:{}, options: {} )
      # if query[:backlog][:url] contains a value. This is where the previous process ended
      if url.nil?
        raise ("missing url for create_backlog_url")
      end
      if query[:backlog][:url] =~ URI::regexp
        @logger.warn "Continue were the previous unfinished backlog process ended"
        url = query[:backlog][:url]
      else
        url = create_url( url: url, query: query, options: options)
      end
      url
    end

    def create_url( url: nil, query:{}, options: {} )
      unless query.empty?
        if @config[:encode_query_value]
          options[:query] = URI.encode_www_form_component( query[:query][:value] )
        else
          options[:query] =  query[:query][:value] 
        end 
      end

      url.scan(/\{\{([^{}]*)\}\}/).each { |substitution|
        substitution = substitution[0]
        if  options[substitution.to_sym].nil?
          raise ("Missing option \"#{ substitution }\" to substitute")
        else
          url = url.gsub(/\{\{#{substitution}\}\}/, options[substitution.to_sym].to_s )
        end
      }
      url
    end
  end
  module Utils
=begin    
    def prefix_datasetid (dataset) 
      prefixed_dataset = dataset.dup
      prefixed_dataset["@id"] = "iCANDID_Dataset_#{dataset[:@id]}"
      prefixed_dataset
    end

    def type_mapping( type )
      default_type = 'Book'
      types = {
          "multivolume monograph" => "Book",
          "publicationvolume" => "PublicationVolume",
          "publicationissue" => "PublicationIssue",
          "book" => "Book"
      }
      if type.nil?
          return default_type
      else
          types[type.downcase] unless type.nil?
      end
    end

    def build_dateline(location, date_published)
      unless location.nil? || date_published.nil?
          "#{location.join(', ')}, #{  date_published  }"
      else
          unless location.nil?
              location.join(', ')
          end
      end
    end

    def build_gender(gender)
      case gender.downcase 
      when "man"; "Male"
      when "vrouw"; "Female"
      else "X"
      end
    end

    def build_duration(seconds)
      # TODO Opsplitsen van seconds in hours minutes and seconds 
      #def seconds_to_hms(sec)
      #    "%02d:%02d:%02d" % [sec / 3600, sec / 60 % 60, sec % 60]
      # end


      ISO8601::Duration.new( seconds.to_i ).to_s
    end

    def build_occupation(occupations)
      occupations.map! do |occupation|
          unless occupation.nil?
              occupation = {
                  :@type => "Occupation",
                  :name =>occupation
              }
          end
      end
      occupations.reject! { |o| o.nil? }
      occupations
    end

    def build_headline(headline, headline2, byline, content_first)
      #@logger.debug( "headline #{ headline }" )
      #@logger.debug( "headline2 #{ headline2 }" )   
      #@logger.debug( "byline #{ byline }" )   
      #@logger.debug( "content_first #{content_first }" )   

      if (headline.first.nil? || headline.empty?) && (headline2.first.nil? ||headline2.empty?)
          if (byline.first.nil? || byline.empty?)
              # @logger.debug(" --- build_headline !!!! content_first #{content_first} ")
              content_first.to_s.truncate( 150, separator: ' ')
          else
              byline.first.to_s.truncate( 150, separator: ' ')
          end
      else
          unless  (headline.first.nil? || headline.empty?) || (headline2.first.nil? || headline2.empty?)
              "#{headline.first}, #{headline2.first}"
          end
          if (headline.first.nil? || headline.empty?) 
              headline2.first
          else
              headline.first
          end
      end
    end
=end

    def self.mailErrorReport (subject,  report , importance, config)
      to_address = ADMIN_MAIL_ADDRESS 
      from_address = "icandid@libis.kuleuven.be"
      now = DateTime.now
  
      message = <<END_OF_MESSAGE
From: #{from_address}
To: #{to_address}
MIME-Version: 1.0
Content-type: text/html
Subject: #{subject}
importance: #{importance}
Date: #{ now }

<H1>#{subject}</H1>

#{report}

END_OF_MESSAGE

      Net::SMTP.start('smtp.kuleuven.be', 25) do |smtp|
          smtp.send_message message,
          from_address, to_address
      end
    end

  end


  module Extract
    
    def self.extract_records( input_file: nil, dest: nil )
      begin
        if input_file.nil?
          raise ("No input_file given for extract_records")
        end

        files_to_process = [input_file]

        unless dest.nil?
          dest =  File.join( File.dirname(input_file), "temp", Time.now.utc.strftime("%Y%m%d%H%M%S") )
        end

        if input_file =~ /.+(\.tar|\.gz|\.tgz)$/
          case input_file
          when /.+\.tar\.gz$/
            unzipped_file = File.join(dest, File.basename(input_file, '.gz'))
            file_to_extract = unzipped_file
          when /.+\.tgz$/
            unzipped_file = File.join(dest, File.basename(input_file, '.tgz'), "tar")
            file_to_extract = unzipped_file
          when /.+\.tar$/
            file_to_extract = input_file
          when /.+\.gz$/
            unzipped_file = File.join(dest, File.basename(input_file, '.gz'))
          end

          if (  !unzipped_file.nil? && !file_to_extract.nil? )
            untar( ungzip(  File.open( input_file ) ) , dest)
          else

            if !unzipped_file.nil?
              FileUtils.mkdir_p(dest)
              File.open(unzipped_file, "w") do |output_stream|
                IO.copy_stream( ungzip( File.open( input_file ) ) , output_stream)
              end
            end

            if ! file_to_extract.nil?
              untar(  ungzip(  File.open( input_file ) )  , dest)
            end
          end

          files_to_process = Dir[  File.join( dest ,"*") ]
          
        end

        pp files_to_process
        return files_to_process
      rescue StandardError => e
        puts e
        raise " ERROR extracting #{input_file}"
      end
    end

    # Creates a tar file in memory recursively
    # from the given path.
    #
    # Returns a StringIO whose underlying String
    # is the contents of the tar file.
    def self.tar(path)
      tarfile = StringIO.new("")
      Gem::Package::TarWriter.new(tarfile) do |tar|
        Dir[File.join(path, "**/*")].each do |file|
          mode = File.stat(file).mode
          relative_file = file.sub /^#{Regexp::escape path}\/?/, ''
          
          if File.directory?(file)
            tar.mkdir relative_file, mode
          else
            tar.add_file relative_file, mode do |tf|
              File.open(file, "rb") { |f| tf.write f.read }
            end
          end
        end
      end
      
      tarfile
    end
    
    # gzips the underlying string in the given StringIO,
    # returning a new StringIO representing the 
    # compressed file.
    def self.gzip(tarfile)
      gz = StringIO.new("")
      z = Zlib::GzipWriter.new(gz)
      z.write tarfile.string
      z.close # this is necessary!
      
      # z was closed to write the gzip footer, so
      # now we need a new StringIO
      StringIO.new gz.string
    end
    
    # un-gzips the given IO, returning the
    # decompressed version as a StringIO
    def self.ungzip(tarfile)
      z = Zlib::GzipReader.new(tarfile)
      unzipped = StringIO.new(z.read)
      z.close
      unzipped
    end

    # untars the given IO into the specified
    # directory
    def self.untar(io, destination)
      Gem::Package::TarReader.new io do |tar|
        tar.each do |tarfile|
          destination_file = File.join destination, tarfile.full_name
          
          if tarfile.directory?
            FileUtils.mkdir_p destination_file
          else
            destination_directory = File.dirname(destination_file)
            FileUtils.mkdir_p destination_directory unless File.directory?(destination_directory)
            File.open destination_file, "wb" do |f|
              f.print tarfile.read
            end
          end
        end
      end
    end
  end

end


