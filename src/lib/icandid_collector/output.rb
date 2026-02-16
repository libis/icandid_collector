#encoding: UTF-8

module IcandidCollector

  class Output
    attr_accessor :icandid_config,  :data

    def initialize( icandid_config: {}, data: {} ) 
      @logger = Logger.new(STDOUT)
      @logger.level = Logger::INFO
      @icandid_config = icandid_config
      @data = HashWithIndifferentAccess.new(data)
    end

    def save_data_to_uri( uri: nil, options: {} )
      begin
        if uri.nil?
          raise "url is required to save_data_to_uri"
        end
        data = {}
        if @data[:data]
          data[:data] = @data[:data]
        end
        if @data[:metadata]
          data[:metadata] = @data[:metadata]
        end
        if data.nil?
          data = @data
        end
        
        out = DataCollector::Output.new( data )
        out.to_uri(uri, options)
      end
    end

  end
end