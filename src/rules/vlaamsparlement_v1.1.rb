#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

DEBUG = false

RULE_SET_v1_1 = {
    version: "1.1",
    rs_next_value: {
        page: { "$" => lambda { |d,o| 
            o["page"]+1
        }},
        total: "$.count" ,
        lastindex: "$.lastindex"
    },
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
            unless (d["result"].empty? || d["count"] == 0)
                first_id = d["result"].first["id"].split('/').last
                last_id = d["result"].last["id"].split('/').last
                "#{first_id}_#{last_id}.json"
            end
        }}
    },
    rs_raw_data:{
        r_data: { "$" => lambda { |d,o| 
               d
            }
        },        
        error: { "$.errors" => [ lambda { |d,o| 
            d
            }]
        }
    },   
    rs_records: {
        records: { "$" => [ lambda { |d,o|
        
            out = DataCollector::Output.new
            unless d["metatags"].nil?
              rules_ng.run(RULE_SET_v1_1[:rs_record], d, out, o)
            else
                puts "TODO Actualiteitsdebat zijn geharvest op een ander manier !!! ze bezitten ook een ander structuur."
                puts " voorbeeld: https://ws.vlpar.be/e/opendata/jln/1681886 en https://ws.vlpar.be/e/opendata/debat/1681738"
                # pp d.keys()
                pp d["id"]
                pp d["titel"]
            end
            
            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$" => lambda { |d,o| 

            reorgenizeddata = DataCollector::Output.new
            d["metatags"]["metatag"].each { |metatag|
                reorgenizeddata[metatag["name"]] =  metatag["value"]
            }
            reorgenizeddata = reorgenizeddata.raw

            rdata = {}

            #pp d
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_1[:rs_record_data], reorgenizeddata, out, o)
            rdata.merge!(out.data)
          
            o[:index] = 0
            rules_ng.run(RULE_SET_v1_1[:rs_contacttype], reorgenizeddata["opendata"], out, o)
            rdata.merge!(out.data)
            out.clear
            rdata.compact

            pp rdata
            exit
            rdata

            
        } }
    },
    rs_id:{
        id:  {'$.id' =>  lambda { |d,o| 
            "#{o[:ingest_data][:dataset][:@id].downcase}_#{d.split('/').last}-00000"
        }}
    },
    rs_record_data: {
        name:            '$.titel',
        description:     '$.onderwerp',
        keywords:        '$.thema',
        datePublished:   '$.publicatiedatum',
        legislationType: ['$.aggregaat','$.aggregaattype'],
        sameAs:          '$.displayurl',
        publisher:        {'$' => lambda { |d,o| 
            {
                :@type => "Organization",
                :@id   => "iCANDID_ORGANIZATION_VLAAMS_PARLEMENT",
                :name  => "Vlaams Parlement"
            }
        }},
        text:        {'$.document' => lambda { |d,o| 
            if d.match(/plenaire-vergaderingen/)
                d.split('/').last
            else
                d.split('=').last
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