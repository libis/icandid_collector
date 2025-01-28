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

                #options = { id_from_file:  File.basename(o[:file], '.json').split('_')[1..].join('_') }                  
                #options = { id_from_file:  File.basename(o[:file], '.json').split('_')[2] }
                #o[:id_from_file] = options[:id_from_file]

                o[:config][:query][:query][:es_retrieve_id_query_param_procs] = [ o[:config][:query][:query][:es_retrieve_id_query_param_procs] ] unless o[:config][:query][:query][:es_retrieve_id_query_param_procs].is_a?(Array)

                o[:config][:query][:query][:es_retrieve_id_query_param_procs].each{ |e| e.each { |param, param_proc| 
                    o[param.to_sym]  = eval ( param_proc )
                } }

                request_options = {
                    user: ENV['ES_USER'],
                    password: ENV['ES_PASSWORD'],
                    url: "#{ENV['ES_URL']}/#{ENV['ES_INDEX']}/_search?",
                    method: "post",
                    verify_ssl: true,
                    headers: {   "Content-Type" => "application/json" },
                    body: Mustache.render( o[:config][:query][:query][:es_retrieve_id_query], o)
                }


                # pp "============> #{ o[:config][:query] }"

                request_options[:user]     = o[:config][:query][:user] unless o[:config][:query][:user].nil?
                request_options[:password] = o[:config][:query][:password] unless o[:config][:query][:password].nil?
                request_options[:url]      = "#{ENV['ES_URL']}/#{ o[:config][:query][:es_index] }/_search?" unless o[:config][:query][:es_index].nil?

                # pp "============> #{request_options[:url] }"

                data = icandid_input.collect_data_from_uri( url: request_options[:url]  ,  options: request_options )
            
                unless data.nil?
                    if data["hits"]["total"]["value"] == 0
                        @logger.warn("====> No hits for #{ request_options[:body]  }")
                        request_options[:password] = "*******"
                        @logger.warn("#No link with recond in Elastic index #{request_options}")
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


                            #pp o[:file]

                            #pp o[:id]
                            

                            id = data["hits"]["hits"].select{ |h| 
                                scopeArchiv_ref_code = h["_source"]["identifier"].select { |i| i["@id"] == 'scopeArchiv_ref_code'}
                                pp "Use the record that has a scopeArchiv_ref_code that almost looks like the id from the filename"
                                unless  scopeArchiv_ref_code.nil?
                                    scopeArchiv_ref_code = scopeArchiv_ref_code.first["value"]
                                    letters, numbers = scopeArchiv_ref_code.split('/').last.downcase.match(/(^[a-z]*)([0-9]*)/).captures
                                    scopeArchiv_ref_code = "#{letters}#{numbers.rjust(6, '0')}"
                                end
                                scopeArchiv_ref_code == o[:id]
                            }.first["_source"]["@id"]
                            pp "id: #{id}"
                            pp "Must  be fixed somewhere else"
                            unless id.nil?
                                rdata = { 
                                    "@id": File.basename(o[:file], '.json'),
                                    "file_generatedAtTime": File.ctime(o[:file]),
                                    "_source": { 
                                        "@id": id,
                                        "texts":   d["texts"],
                                        "objects": d["objects"]
                                    }       
                                }
                            else
                                raise "request_options is link to multiple records !!!!!!!! \n #{request_options}"
                            end
                        end
                    end
                end

            end

            rdata
        } ] }
    }
}
