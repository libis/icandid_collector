#encoding: UTF-8
require 'data_collector'
require "iso639"

def get_uuid (uuid_url)
    begin
        http = HTTP
        uri = URI.decode_www_form_component("#{uuid_url.to_s}")

        http_response = http.follow.get(uri.to_s, {})

        data = JSON.parse( http_response.body.to_s )

        case http_response.code
        when 200..299
            uuid = data
            url = "https://icandid.libis.be/_/" + uuid
        when 400
            uuid = data["uuid"]
            url = "https://icandid.libis.be/_/" + uuid
        end
        [ url, uuid ]
    rescue StandardError => e 
        pp "rescue rescue rescuerescue"
        pp e

    end
end

RULE_SET_BASIC_ICANDID = {
    version: "1.0",
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o| 

            # https://www.w3.org/TR/json-ld/#advanced-context-usage
            # https://github.com/schemaorg/schemaorg/issues/1905

            if Iso639[o[:ingest_data][:metaLanguage]].nil?
                puts ""
                puts ""
                puts ""
                pp "CHECK o[:ingest_data][:metaLanguage]: #{o[:ingest_data][:metaLanguage]}"
                puts ""
                exit
            end

            o[:uuid_generate] = {
                url: "https://services6.libis.be/uuid/generate",
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

            id = "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{o[:id]}"
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
                    :@language => "#{ o[:ingest_data][:metaLanguage] }-#{ o[:ingest_data][:unicode_script]}",
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
