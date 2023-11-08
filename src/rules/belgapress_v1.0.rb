#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data], d, out, o)
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
# categories, keywords, topic, entities

            if rdata[:headline].empty?
                rdata[:headline] = rdata[:articleBody].to_s.truncate( 150, separator: ' ') unless (rdata[:articleBody].empty?)
            end
            rdata[:name] = rdata[:headline] 
            
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

#            if rdata[:@id]  == "iCANDID_belgapress_belgapress_query_00008_61b533f3-7832-4c36-bda4-4acf8fc55429-00000"
#                puts rdata[:headline].codepoints
#            end

            if (d["mediumType"] == "NEWSPAPER" && d["mediumTypeGroup"] == "PRINT")
                rdata[:@type ] = "NewsArticle"
            end
            if (d["mediumType"] == "NEWSPAPER" && d["mediumTypeGroup"] == "ONLINE")
                rdata[:@type ] = "WebSite"
            end

            rules_ng.run(RULE_SET_v1_0[:rs_printEdition], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_articleSection], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_associated], d, out, o)         
            rdata.merge!(out.to_h)
            out.clear

            # DOTO (split ?)
            # data input examples :
            #  "authors"=> ["Journaliste de la cellule wallonne Par Jean-Philippe de Vogelaere"],
            #  "authors"=>["Par Benjamin Quenelle Par Christophe Bourdoiseau"],
            
            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_creator], d, out, o)
            rdata.merge!(out.to_h)
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
                :@id            => "#{o[:prefixid]}_#{ d["uuid"] }-00000",
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
