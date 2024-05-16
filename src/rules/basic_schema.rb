#encoding: UTF-8
require 'data_collector'
require "iso639"

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


            id = "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{o[:id]}"
            uuid = nil
            url = nil

            uuid_url = o[:uuid_generate][:url] +"/"+ id +"?by="+ o[:uuid_generate][:by] +"&for="+ o[:uuid_generate][:for] +"&resolvable="+ o[:uuid_generate][:resolvable]
  
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

 
            {


                :@id            => id,
                :@uuid          => uuid,
                :url            => url,
                :@type          => o[:type],
                :additionalType => "CreativeWork",
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
                :@context  => {
                    :@vocab => "https://schema.org/",
                    :@language => "#{ o[:ingest_data][:metaLanguage] }-#{ o[:ingest_data][:unicode_script]}",
                    :prov => "https://www.w3.org/ns/prov#",
                    :"prov:wasAssociatedFor" => {
                        :@reverse => "prov:wasAssociatedWith"
                    }
                }
            }
        
        }}
    }
}
