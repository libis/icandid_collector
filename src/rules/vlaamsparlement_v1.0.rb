#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data], d, out, o)
            out[:data] 
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|



#pp d.keys
#pp d["document"]
#pp d["euro-document"]
#pp d["procedureverloop"].select { |v| v["status"] == "ingediend" }.map{ |r| r["datum"]}
#pp d["thema"]
#pp d["onderwerp"]

            rdata = {
                :name       => d["titel"],
                :description => d["onderwerp"],
                :keywords   => d["thema"], 
                :datePublished => d["procedureverloop"].select { |v| v["status"] == "ingediend" }.map{ |r| r["datum"]},
                :legislationType => d["objecttype"]["naam"],
                :sameAs     => "https://www.vlaamsparlement.be/nl/parlementaire-documenten/parlementaire-initiatieven/#{d["id"]}"
            }

            if d["document"]
                rdata[:text] = "#{d["id"]}_#{d["document"]["bestandsnaam"]}"
            end
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_contacttype], d, out, o)
            rdata.merge!(out.data)
            out.clear

            #    o[:index] = 0
        #    rules_ng.run(RULE_SET_v1_0[:rs_associated], d, out, o)         
        #    rdata.merge!(out.to_h)
        #    out.clear

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
            rules_ng.run(RULE_SET_v1_0[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        legislationResponsible: { "$.contacttype[?(@.beschrijving == 'Bevoegde minister')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        author: { "$.contacttype[?(@.beschrijving == 'Verslaggever')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_contact],  d["contact"], out, o)
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
                    :@type => "Organisation",
                    :@id   => "#{o[:@id]}_ORGANISATION_#{ o[:index] }",
                    :name  => d["naam"]
                }
            end  
            if d["fractie"] 
                rdata["memberOf"] = {
                    :@type => "Organisation",
                    :@id   => "#{o[:prefixid]}__ORGANISATION__id_#{d["fractie"]["id"] }",
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
