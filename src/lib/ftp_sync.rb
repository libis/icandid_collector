#encoding: UTF-8
require 'net/ftp'
require 'rubygems'
require 'net/ftp/list'
require 'fileutils'

# A Ruby library for recursively downloading and uploading directories to/from ftp
# servers. Also supports uploading and downloading a list of files relative to 
# the local/remote roots. You can specify a timestamp to only download files 
# newer than that timestamp, or only download files newer than their local copy.
class FtpSync
  
  attr_accessor :verbose, :server, :user, :password, :passive
  
  # Creates a new instance for accessing a ftp server 
  # requires +server+, +user+, and +password+ options
  # * :ignore - Accepts an instance of class which has an ignore? method, taking a path and returns true or false, for whether to ignore the file or not.
  # * :verbose - Whether should be verbose
  def initialize(server, user, password, options = {})
    @server = server
    @user = user
    @password = password
    @connection = nil
    @ignore = options[:ignore]
    @recursion_level = 0
    @verbose = options[:verbose] || false
    @passive = options[:passive] || false
  end
  
  def download_zips(localpath, remotepath, options = {}, &block)
    options[:filepattern] = /\.zip$/
    download(localpath, remotepath, options, &block)
  end

  # Recursively pull down files
  # :since => true - only pull down files newer than their local counterpart, or with a different filesize
  # :since => Time.now - only pull down files newer than the supplied timestamp, or with a different filesize
  # :filepattern => regex to filter files that have to be downloaded
  # If a block is supplied then it will be called to remove a local file
  def download(localpath, remotepath, options = {}, &block)

    connect! unless @connection
   
    tocopy = []

    # To trigger error if path doesnt exist since list will
    # just return and empty array
    @connection.chdir(remotepath) 

    @connection.list(remotepath) do |e|
      
      entry = Net::FTP::List.parse(e)

      paths = [ File.join(localpath, entry.basename), "#{remotepath}/#{entry.basename}".gsub(/\/+/, '/') ]

      if entry.file? && entry.basename.match( options[:filepattern] )
        if options[:since]
          puts "entry.mtime #{ entry.mtime }"
          puts "File.mtime(paths[0]) #{ File.mtime(paths[0]) }" unless ! File.exist?(paths[0]) 
          puts "entry.filesize #{ entry.filesize }" 
          puts "File.size(paths[0]) #{ File.size(paths[0]) }" unless ! File.exist?(paths[0]) 
          
          tocopy << paths unless File.exist?(paths[0]) and entry.mtime < File.mtime(paths[0]) and entry.filesize == File.size(paths[0])
        elsif options[:since].is_a?(Time)
          tocopy << paths unless entry.mtime < options[:since] and File.exist?(paths[0]) and entry.filesize == File.size(paths[0])
        else
          tocopy << paths
        end
      end
    end
  
    tocopy.each do |paths|
      localfile, remotefile = paths
      unless should_ignore?(localfile)
        begin
          @connection.get(remotefile, localfile)
          log "Pulled file #{remotefile}"
        rescue Net::FTPPermError
          log "ERROR READING #{remotefile}"
          raise Net::FTPPermError unless options[:skip_errors]
        end        
      end
    end

    
  rescue Net::FTPPermError
    close!
    raise Net::FTPPermError
  end

  def close
    close!
  end

  # Chains off to the (if supplied) Ignore class, ie GitIgnores.new.ignore?('path/to/my/file')
  def should_ignore?(path)
    @ignore && @ignore.ignore?(path)
  end
  
  private
    def connect!
      @connection = Net::FTP.new(@server)
      @connection.passive = @passive
      @connection.login(@user, @password)
      log "Opened connection to #{@server}"
    end
  
    def close!
      @connection.close
      log "Closed Connection to #{@server}"
    end
    
    def log(msg)
      puts msg if @verbose
    end
end