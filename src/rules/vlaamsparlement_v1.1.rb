#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_1 = {
    version: "1.1",
    rs_records: {
        records: { "$" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            unless d["metatags"].nil?
                rules_ng.run(RULE_SET_v1_1[:rs_data], d, out, o)
            else
                puts "TODO Actualiteitsdebat zijn geharvest op een ander manier !!! ze bezitten ook een ander structuur."
                puts " voorbeeld: https://ws.vlpar.be/e/opendata/jln/1681886 en https://ws.vlpar.be/e/opendata/debat/1681738"
                # pp d.keys()
                pp d["id"]
                pp d["titel"]

            end
            out[:data] 
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|

            reorgenizeddata = DataCollector::Output.new
            d["metatags"]["metatag"].each { |metatag|
                reorgenizeddata[metatag["name"]] =  metatag["value"]
            }

            data = reorgenizeddata.raw
            data["id"] = d["id"].split('/').last

#pp d["document"]
#pp d["euro-document"]
#pp d["procedureverloop"].select { |v| v["status"] == "ingediend" }.map{ |r| r["datum"]}
#pp d["thema"]
#pp d["onderwerp"]

            rdata = {
                :name            => data["titel"],
                :description     => data["onderwerp"],
                :keywords        => data["thema"], 
                :datePublished   => data["publicatiedatum"],
                :legislationType => [data["aggregaat"],data["aggregaattype"]],
                :sameAs          => data["displayurl"],
                :publisher       => {
                    :@type => "Organization",
                    :@id   => "iCANDID_ORGANIZATION_VLAAMS_PARLEMENT",
                    :name  => "Vlaams Parlement"
                }
            }

            if data["document"]
                pp data["document"]
                if data["document"].match(/plenaire-vergaderingen/)
                    id = data["document"].split('/').last
                else
                    id = data["document"].split('=').last
                end
                rdata[:text] = id
            end

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_basic_schema], data, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

            rules_ng.run(RULE_SET_v1_1[:rs_in_language], data, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_1[:rs_contacttype], data["opendata"], out, o)
            rdata.merge!(out.data)
            out.clear
            rdata.compact
        }
      }
    },

    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  

            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end
            {
                :@id            => "#{o[:prefixid]}_#{ d["id"] }-00000",
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{ INGEST_CONF[:prefixid] }_#{  INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }",
                    :name     => INGEST_CONF[:genericRecordDesc],
                    :provider => INGEST_CONF[:provider],
                    :isPartOf => {
                        :@id   => INGEST_CONF[:dataset][:@id].downcase,
                        :@type => "Dataset",
                        :name  => INGEST_CONF[:dataset][:name],
                        :license  => INGEST_CONF[:dataset][:license]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{ language }-#{ INGEST_CONF[:unicode_script]}" }]    
            }
        }}
    },
    rs_in_language: {
        inLanguage: { "$.language" =>  lambda { |d,o| 
            unless Iso639[d].nil? || Iso639[d].alpha2.to_s.empty?
                {
                    :@type         => "Language",
                    :@id           => Iso639[d].alpha2,
                    :name          => Iso639[d].name,
                    :alternateName => Iso639[d].alpha2,
                }
            else
                {
                    :@type => "Language",
                    :name => "Undetermined",
                    :alternateName => "und",
                    :@id => "und"
                }
            end
        }}
    },
    rs_contacttype: {
        legislationPassedBy: { "$.contacttype[?(@.beschrijving == 'Indiener')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        legislationResponsible: { "$.contacttype[?(@.beschrijving == 'Bevoegde minister')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        author: { "$.contacttype[?(@.beschrijving == 'Verslaggever')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }}
    }, 
    rs_contact: {
        contact: { "$" =>  lambda { |d,o|
            if d["id"]
                rdata = {
                    :@type => "Person",
                    :@id   => "#{o[:prefixid]}_PERSON_id_#{ d["id"] }",
                    :name  => "#{d["voornaam"]} #{d["naam"]}"
                }
            else    
                rdata = {
                    :@type => "Organization",
                    :@id   => "#{o[:@id]}_ORGANIZATION_#{ o[:index] }",
                    :name  => d["naam"]
                }
            end  
            if d["fractie"] 
                rdata["memberOf"] = {
                    :@type => "Organization",
                    :@id   => "#{o[:prefixid]}__ORGANIZATION__id_#{d["fractie"]["id"] }",
                    :name  => d["fractie"]["naam"]
                }
            end
            rdata
        }}
    },
    rs_associated: {
        associatedMedia:  { "$.attachments" => lambda { |d,o|  
            m = {
                :@type         => "MediaObject",
                :@id           => "#{o[:@id]}_MEDIA_#{ o[:index] }",
                :caption       => d["title"],
                :width         => d["width"],
                :height        => d["height"],
                :duration      => d["duration"],
                :author        => d["credit"],
                :thumbnailUrl  => d["references"].select{ |r| r["representation"] == "SMALL"}.map{ |r| r["href"]},
                :contentUrl    => d["references"].select{ |r| r["representation"] == "ORIGINAL"}.map{ |r| r["href"]},
                :encodingFormat => d["references"].select{ |r| r["representation"] == "SMALL"}.map{ |r| r["mimeType"] }
            }
            o[:index] =  o[:index]+1
            
            case d["type"].downcase 
            when "image", "page"
                m[:@type] = "ImageObject"
                m.delete(:duration)
                if m[:encodingFormat].include?("PNG")
                    m[:encodingFormat] = "image/png"
                end
            when "video"
                m[:@type] = "VideoObject"
            end

            m.compact
        }}
    }

}
