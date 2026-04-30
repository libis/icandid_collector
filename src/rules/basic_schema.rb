#encoding: UTF-8
require 'data_collector'
require "iso639"

def get_uuid (uuid_url)
    begin
        http = HTTP
        uri = URI.decode_www_form_component("#{uuid_url.to_s}")
        
        http_response = http.follow.get(uri.to_s, {})

        uuid_data = JSON.parse( http_response.body.to_s )
        
        case http_response.code
        when 200..299
            if uuid_data.has_key?("uuid")
                uuid = uuid_data["uuid"]
            elsif uuid_data.has_key?("string")
                uuid = uuid_data["string"]
            else 
                rasise "Error [#{http_response.status}] while creating UUID with #{uuid_url}"
            end
        when 400
            if uuid_data.has_key?("created") &&  uuid_data.has_key?("to_uuid") &&  uuid_data.has_key?("uuid")
                uuid = uuid_data["to_uuid"]
            elsif uuid_data.has_key?("created") &&  uuid_data.has_key?("from_uuid") &&  uuid_data.has_key?("uuid")
                uuid = uuid_data["uuid"]
            else
                pp "uuid_data:::::::::"
                pp uuid_data
                raise "Error while creating UUID in basic_schema.rb rules"
            end
        end
        # URI.join(o[:ingest_data][:url_prefix]  "_/", uuid).to_s, 
        url = "https://icandid.libis.be/_/" + uuid      
        [ url, uuid ]
    rescue StandardError => e 

        pp "rescue [get_uuid in basic_scema]"
        pp e
        raise e
    end
end

RULE_SET_BASIC_ICANDID = {
    version: "1.0",
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o| 

            # https://www.w3.org/TR/json-ld/#advanced-context-usage
            # https://github.com/schemaorg/schemaorg/issues/1905

            unless Iso639[o[:ingest_data][:metaLanguage]].nil? || o[:ingest_data][:metaLanguage] == "und"
                langcode = o[:ingest_data][:metaLanguage]
            else
                unless Iso639[o[:detectedLanguage]].nil?
                    langcode = o[:detectedLanguage]
                else
                    puts "" 
                    puts ""
                    puts ""
                    pp "CHECK o[:ingest_data][:metaLanguage]: #{o[:ingest_data][:metaLanguage]}"
                    pp o
                    puts ""
                    raise "No valid language code found in ingest_data[:metaLanguage] #{o[:ingest_data][:metaLanguage]} or detectedLanguage #{o[:detectedLanguage]} for #{o[:id]}"
                end
            end

            o[:uuid_generate] = {
                url: "https://services.libis.be/uuid/generate",
                by: "icandid_tech@libis.kuleuven.be",
                for: "icandid",
                resolvable: "1"
            }

            if o[:additionalType].nil?
                    o[:additionalType] = [ "CreativeWork" ]
            end

            
            unless o[:additionalType].is_a?(Array)
                o[:additionalType] = ( o[:additionalType] )
            end
            
            unless o[:additionalType].include?("CreativeWork")
                o[:additionalType] << "CreativeWork"
            end

            
            #id = o[:id].to_s.empty? ? "#{o[:ingest_data][:prefixid]}_#{o[:ingest_data][:provider][:@id].downcase}_#{o[:id]}" : o[:id]
            if o[:ingest_data][:provider][:@id] == "ENA"
                id = "#{o[:ingest_data][:prefixid]}_#{o[:ingest_data][:provider][:@id]}_#{o[:id]}"
            else
                id = "#{o[:ingest_data][:prefixid]}_#{o[:ingest_data][:provider][:@id].downcase}_#{o[:id]}"
            end
            uuid = nil
            url = nil

            uuid_url = o[:uuid_generate][:url] +"/"+ id +"?by="+ o[:uuid_generate][:by] +"&for="+ o[:uuid_generate][:for] +"&resolvable="+ o[:uuid_generate][:resolvable]
  
            url, uuid = get_uuid(uuid_url)
 
            {
                :@id            => id,
                :@uuid          => uuid,
                :url            => url,
                :@type          => o[:type],
                :additionalType => o[:additionalType],
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{ o[:ingest_data][:prefixid] }_#{  o[:ingest_data][:provider][:@id].downcase }_#{ o[:ingest_data][:dataset][:@id].downcase }",
                    :name     => o[:ingest_data][:genericRecordDesc],
                    :provider => o[:ingest_data][:provider],
                    :isPartOf => {
                        :@id   => o[:ingest_data][:dataset][:@id].downcase,
                        :@type => "Dataset",
                        :name  => o[:ingest_data][:dataset][:name],
                        :license  => o[:ingest_data][:dataset][:license]
                    }
                },
                # Also check CONTEXT in icandid_utils in ES_LOADER !!!
                :@context  => {
                    :@vocab => "https://schema.org/",
                    :prov => "https://www.w3.org/ns/prov#",
                    :@language => "#{ langcode }-#{ o[:ingest_data][:unicode_script]}",
                    :"prov:wasAssociatedFor" => {
                        :@reverse => "prov:wasAssociatedWith"
                    }

                }
            }.compact
        
        }}
    }
}


#####################################################################################################
# Define @context
# https://www.w3.org/TR/2014/REC-json-ld-20140116/#advanced-context-usage
# https://stackoverflow.com/questions/47227586/multiple-contexts-in-json-ld
#
# Reverse Properties
# https://www.w3.org/TR/json-ld/#reverse-properties
# 
# Using properties from mutiple type (example color from Product used in Image, ImageObject, ...)
# https://schema.org/additionalType
# 
# Additionalproperty 
# https://victorious.com/blog/product-schema-markup-example/
# 
#####################################################################################################
