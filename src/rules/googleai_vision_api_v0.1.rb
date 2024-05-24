#encoding: UTF-8
require 'data_collector'
require "iso639"

GOOGLE_AI_VISION_API_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "@" => [ lambda { |d,o| 


            icandid_input  = IcandidCollector::Input.new()
            output = DataCollector::Output.new
            begin

                options = { id_from_file:  File.basename(o[:file], '.json').split('_')[1..].join('_') }                  

                request_options = {
                    user: ENV['ES_USER'],
                    password: ENV['ES_PASSWORD'],
                    url: "#{ENV['ES_URL']}/#{ENV['ES_INDEX']}/_search?",
                    method: "post",
                    verify_ssl: true,
                    headers: {   "Content-Type" => "application/json" },
                    body: Mustache.render( o[:config][:query][:query][:es_retrieve_id_query], options)
                }

                data = icandid_input.collect_data_from_uri( url: request_options[:url]  ,  options: request_options )
            
                unless data.nil?

                    if data["hits"]["total"]["value"] == 0
                        pp options[:id_from_file]
                        @logger.error("====> No hits for #{ options[:id_from_file]  }")
                        request_options[:password] = "*******"
                        @logger.error("#{options[:id_from_file]} has no link with recond in Elastic index #{request_options}")

                    else
                        if data["hits"]["total"]["value"] == 1
                            rdata = { 
                                "@id": File.basename(o[:file], '.json'),
                                "file_generatedAtTime": File.ctime(o[:file]),
                                "_source": { 
                                    "@id": data["hits"]["hits"].first["_source"]["@id"],
                                    "texts":   d["texts"],
                                    "objects": d["objects"]
                                }       
                            }
                        else
                            pp " data[\"hits\"][\"total\"][\"value\"] #{data["hits"]["total"]["value"]}"
                            pp o[:file]
                            raise "#{options[:id_from_file]} is link to multiple records !!!!!!!! \n #{request_options}"
                        end
                    end
                end

            end


            rdata
        } ] }
    }
}
