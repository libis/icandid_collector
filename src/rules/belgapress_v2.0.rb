#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

DEBUG = true

RULE_SET_v2_0 = {
    version: "2.0",
    rs_next_value: {
        next_url: { "$._links.next" => [ lambda { |d,o| 
                d
            }]
        },
        total: { "$._meta.total" => [ lambda { |d,o| 
                d
            }]
        }
    },
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
            unless (d["data"].empty? && d["_meta"]["total"] == 0)
                "#{d["data"].first["uuid"]}_#{d["data"].last["uuid"]}.json"
            end
        }}
    },
    rs_raw_data:{
        data: { "$.data" => lambda { |d,o| 
               d
            }
        },
        error: { "$.error" => lambda { |d,o| 
            d
            }
        }
    },    
    rs_records: {
        records: { "$.data" => [ lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_0[:rs_data], d, out, o)
            data = out[:data]
            data
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|
            rdata = {
                :headline    => Nokogiri::HTML( d["title"] ).text.strip.gsub(160.chr("UTF-8"),""),
                :description => Nokogiri::HTML( d["lead"] ).text.strip.gsub(160.chr("UTF-8"),""),
                :articleBody => Nokogiri::HTML( d["body"] ).text.strip.gsub(160.chr("UTF-8"),""),
                :datePublished => d["publishDate"],
                :publisher     => {
                    :@type => "Organization",
                    :@id   => I18n.transliterate( d["source"] ).delete(' ').delete('\''),
                    :name  => d["source"],
                    :logo  => d["sourceLogo"]
                },

                :pageStart      => d["page"],
                :pageEnd        => d["page"],
                :pagination     => "#{d["page"]}-#{d["page"]}",

                :wordCount      => d["wordCount"],
                :keywords       => d["topic"],

                :sameAs => "https://share.belga.press/news/#{d["uuid"]}"
            }

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_0[:rs_id], d, out, o)
            o[:id] = out[:id].first

            
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear
         
# categories, keywords, topic, entities

            if d["source"] == "BELGA AUDIO"
                if rdata[:headline] =~ /belga (\d{2}):(\d{2})/
                    rdata[:headline] = "Belga nieuws #{ Date.parse( d["publishDate"]).strftime('%d/%m/%Y') } #{$1}:#{$2}"
                end
                if rdata[:headline] =~ /Nieuws (\d{2})u(\d{2})/
                   rdata[:headline] = "Belga nieuws #{ Date.parse( d["publishDate"]).strftime('%d/%m/%Y') } #{$1}:#{$2}"
                end
            end
            if rdata[:headline].empty?
                rdata[:headline] = rdata[:articleBody].to_s.truncate( 150, separator: ' ') unless (rdata[:articleBody].empty?)
            end
            rdata[:name] = rdata[:headline]


#            if rdata[:@id]  == "iCANDID_belgapress_belgapress_query_00008_61b533f3-7832-4c36-bda4-4acf8fc55429-00000"
#                puts rdata[:headline].codepoints
#            end

            if (d["mediumType"] == "NEWSPAPER" && d["mediumTypeGroup"] == "PRINT")
                rdata[:@type ] = "NewsArticle"
            end
            if (d["mediumType"] == "NEWSPAPER" && d["mediumTypeGroup"] == "ONLINE")
                rdata[:@type ] = "WebSite"
            end

            rules_ng.run(RULE_SET_v2_0[:rs_printEdition], d, out, o)
            rules_ng.run(RULE_SET_v2_0[:rs_articleSection], d, out, o) unless d['source'].starts_with?("BELGA")
            rules_ng.run(RULE_SET_v2_0[:rs_in_language], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v2_0[:rs_associated], d, out, o)
            rdata.merge!(out.to_h)
            out.clear

            # DOTO (split ?)
            # data input examples :
            #  "authors"=> ["Journaliste de la cellule wallonne Par Jean-Philippe de Vogelaere"],
            #  "authors"=>["Par Benjamin Quenelle Par Christophe Bourdoiseau"],

            o[:index] = 0
            rules_ng.run(RULE_SET_v2_0[:rs_creator], d, out, o)
            rdata.merge!(out.to_h)
            out.clear

            rdata.compact
        }
      }
    },
    rs_id:{
        id:  {'$.uuid' =>  lambda { |d,o| 
            "#{ d }-00000"
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
    rs_printEdition: {
        printEdition: { "$.editions" =>  lambda { |d,o|
           d.split(/,[\s]*/)
        }}
    },
    rs_articleSection: {
        articleSection: { "$.subSource" =>  lambda { |d,o|
            d
        }}
    },
    rs_creator: {
        creator: { "$.authors" =>  lambda { |d,o|
            rdata = {
                :@type => "Person",
                :@id   => "#{o[:@id]}_PERSON_#{ o[:index] }",
                :name  => d
            }
            o[:index] =  o[:index]+1
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
