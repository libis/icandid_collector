
require 'elasticsearch'

class Elastic

    attr_accessor :config, 
    :log_file, #log file.
    :logger,
    :es_url,
    :es_version,
    :es_index,
    :es_cluster,
    :es_pipeline_id,


  def initialize()


    
    Encoding.default_external = "UTF-8"

    #@log_file             = './logs/es_loader.log'
    @log_file             = STDOUT
    @log_level            = Logger::DEBUG    
    @logger               = Logger.new(@log_file)

    @log_es_client        = true
    #@client_logger        = Logger.new('./logs/es_client.log')
    @client_logger        = Logger.new(STDOUT)

    @config_file          = "config.yml"

    @jsonoutput           = []

    @es_version           = nil
    @es_url               = nil
    @es_cluster           = nil
    @es_index             = nil
    @es_pipeline_id       = nil

    @es_client            = nil

  end

  def check_elastic()

    pp @es_url

    if @es_url.to_s.empty?
      raise 'es_url not defined'
    end
    @logger.debug "es_url: #{ es_url }"

    @es_client = Elasticsearch::Client.new url: @es_url, logger: @client_logger, log: true, transport_options: { ssl: { verify: false } }
    #@es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }
=begin    
    if @log_es_client
      @es_client = Elasticsearch::Client.new url: @es_url, logger: @client_logger, log: true, transport_options: { ssl: { verify: false } }
    else
      @es_client = Elasticsearch::Client.new url: @es_url, transport_options: {  ssl:  { verify: false } }
    end
=end
    health = @es_client.cluster.health
    @logger.debug "cluster.health.status: #{health['status']}"
    
    if @es_client.info['version']['number'] != @es_version
        message = "Wrong Elasticsearch version on server: #{ @es_client.info['version']['number'] } on server but expected #{ @es_version }"
        @logger.warn message
        raise message
    end

    unless health['status'] === 'green' || health['status'] === 'yellow'
      message = "ElasticSearch Health status not OK [ #{health['status']} ]"
      @logger.error message
      raise message
    end

  end


  def get_document_by_id( index: 'test'  , id: )
    retval = @es_client.get ({index: index, id: id, ignore: 404 } )
    if retval["found"]
      retval["_source"]
    else
      nil
    end
  end

  def load_to_es( jsondata: {}, es_client: @es_client, logger: @logger)
    unless jsondata.empty? 
      logger.debug "load ##{jsondata.size / 2} records to Elastic #{jsondata.size / 2}"
      retval =  @es_client.bulk body: jsondata
      
      if retval['errors']
        error_message = ["!!!! while inserting records !!!!!!!!!!!!!!!!!\nErrors in bulk: #{retval['errors']}"]
        
        retval['items'].each do |i|
          unless i['index']['error'].nil?
            error_message << "Error in bulk items #{i['index']['_id']} : i['update']['error']"
            raise "Error in bulk items : #{i}"
          end
        end
        raise error_message.join("\n")
      end
    end
  end

end