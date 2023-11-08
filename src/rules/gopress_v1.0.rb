#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_0[:rs_data], d["article.published"], out, o)
                data = out[:data]
                data
        } }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|

            title_id       = d["head"]["id"]["_title"]
            scope_id       = d["head"]["id"]["_scope"]
            issue_id       = d["head"]["id"]["_issue"]

            id = "#{title_id}#{scope_id}#{issue_id}#{ DateTime.parse( d["head"]["id"]["_pubdate"] ).strftime("%d%m%Y") }-00000"

            date_published =  DateTime.parse( d["head"]["id"]["_pubdate"] ).strftime("%Y-%m-%d")

            rdata = {
                :datePublished => date_published,
                :publisher     => {
                    :@type => "Organization",
                    :@id   => I18n.transliterate( d["head"]["id"]["_title"] ).delete(' ').delete('\''),
                    :name  => d["head"]["meta"]["publication"]["$text"]
                },
            }

            paper =   d["head"]["id"]["_title"].gsub(/[[:space:]]/, '').downcase

            INGEST_CONF[:dataset] = {
                    "@id": paper,
                    "@type": "Dataset",
                    "name": rdata[:publisher][:name]
            }

            INGEST_CONF[:genericRecordDesc] = "Entry from GoPress - #{  rdata[:publisher][:name]  }"

            o[:@id] = id

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_dateline], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_authortagline], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_attachments], d, out, o)

            rdata.merge!(out.to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_body_head], d, out, o)
            rdata.merge!(out[:body_head].to_h)
                   
            rules_ng.run(RULE_SET_v1_0[:rs_meta], d, out, o)
            rdata.merge!(out[:meta].to_h)

            rules_ng.run(RULE_SET_v1_0[:rs_body_coords], d, out, o)
            if out[:body_coords].kind_of?(Array)
                coord = {
                    :pageStart => out[:body_coords].map{ |c| c[:pageStart]}.min,
                    :pageEnd   => out[:body_coords].map{ |c| c[:pageEnd]}.max
                }
                coord[:pagination] = "#{ coord[:pageStart] }-#{ coord[:pageEnd] }"
                rdata.merge!(coord)
            else
                rdata.merge!(out[:body_coords].to_h)
            end

            rules_ng.run(RULE_SET_v1_0[:rs_body_content], d, out, o)
            rdata.merge!(out[:body_content].to_h)
            out.clear

            if rdata[:headline].nil?
                rdata[:headline] = rdata[:articleBody].first.to_s.truncate( 150, separator: ' ') unless (rdata[:articleBody].nil? || rdata[:articleBody].size != 1)
            else
                rdata[:headline] = rdata[:headline][0]
            end

            rdata[:name] = rdata[:headline]

            rdata[:name] = rdata[:headline]
            if rdata[:articleBody].nil?
                rdata[:articleBody] = rdata[:headline] unless rdata[:headline].nil?
            else
                rdata[:articleBody] = rdata[:articleBody].first unless rdata[:articleBody].size != 1
            end



            rdata[:sameAs] = "http://academic.gopress.be/Public/index.php?page=archive-article&issueDate=#{date_published}&articleOriginalId=#{id}"
            
            rules_ng.run(RULE_SET_v1_0[:rs_attachments], d, out, o)

            out.clear

            #            pp rdata
            rdata.compact
        }
      }
    },
    rs_body_head:{
        body_head: { "$.body.['body.head']" => lambda { |d,o|  
            rdata = {
                :name => ""  
            }
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_headline], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_attachments], d, out, o)
            rdata.merge!(out.to_h)
            rdata
        } }
    },
    rs_meta: {
	meta: { "$.head.meta" => lambda { |d,o|  
            rdata= {
		   :articleSection => d["section"]
	    }

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_edition], d, out, o)
            rdata.merge!(out.to_h)
            rdata

            o[:index] = 0
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_content_location], d, out, o)
            rdata.merge!(out.to_h)
            rdata
        } }
    },
    rs_edition: {
       printEdition: { "$.edition" => lambda { |d,o|  
	       d["$text"].split(/,[\s]*/)
        } }
    },
    rs_body_coords: {
        body_coords: { "$.body.coords" => lambda { |d,o|  
            rdata = {
                :pageStart => d["_pageId"],
                :pageEnd   => d["_pageId"]
            }

            unless rdata[:pageStart].nil?  || rdata[:pageEnd].nil?
                rdata[:pagination] = "#{ rdata[:pageStart]}-#{ rdata[:pageEnd] }"
            end
            rdata
        } }
    },
    rs_body_content:{
        body_content: { "$.body.['body.content']" => lambda { |d,o|  
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_article_body], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_lead], d, out, o)
            out.to_h
        } }
    },
    rs_lead: {
        description: { "$.lead.['body.p']" => lambda { |d,o|  
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_p], d, out, o)
            Nokogiri::HTML(   out[:p].join(" ") ).text
        } }
    },
    rs_article_body:{
        articleBody: { "$.['body.p']" => lambda { |d,o|
            if d["p"].nil?
                Nokogiri::HTML(  d["subtitle"]["p"] ).text unless d["subtitle"].nil?
            else
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_0[:rs_p], d, out, o)
                Nokogiri::HTML(   out[:p].join(" ") ).text
            end
        } }
    },
    rs_p:{
        p: { "$.p" => lambda { |d,o|
                 d
        } }
    },
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            
	    id = o[:@id]

            unless Iso639[d["_xml:lang"]].nil? || Iso639[d["_xml:lang"]].alpha2.to_s.empty?
                language = Iso639[d["_xml:lang"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end

            {
                :@id            => "#{ INGEST_CONF[:prefixid] }_#{  INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }_#{id}",
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{ INGEST_CONF[:prefixid] }_#{  INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }",
                    :name     => INGEST_CONF[:genericRecordDesc],
                    :provider => INGEST_CONF[:provider],
                    :isPartOf => {
                        :@id   => o[:convert_gopress_id_to_belgapress_id][ INGEST_CONF[:dataset][:@id].to_sym ],
                        :@type => "Dataset",
                        :name  => INGEST_CONF[:dataset][:name],
                        :license  => INGEST_CONF[:dataset][:license]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{ language }-#{ INGEST_CONF[:unicode_script]}" }]    
            }
        }}
    },
    rs_headline: {
        headline:  { "$" => lambda { |d,o| 
            headline = headline2 = byline = nil
            unless d["headline"].nil?
                headline  = d["headline"]["hl1"]["p"] unless d["headline"]["hl1"].nil?
                headline2 = d["headline"]["hl2"]["p"] unless d["headline"]["hl2"].nil?
            end
            unless d["byline"].nil?
                if d["byline"]["p"].kind_of?(Array)
                   byline  = d["byline"]["p"].join(', ') 
                else
                    byline = d["byline"]["p"]
                end
                byline = Nokogiri::HTML( byline ).text.truncate( 150, separator: ' ') 
            end
            headline = Nokogiri::HTML( [ headline, headline2 ].reject { |item| item.nil? }.join(', ') ).text 
            headline = [ headline, byline].reject { |item| item.nil? || item.empty? }
            headline = headline.first unless headline.nil?
            headline

        }}
    },
    rs_in_language: {
        inLanguage: { "$._xml:lang" =>  lambda { |d,o| 
            unless Iso639[ d ].nil? || Iso639[ d ] .alpha2.to_s.empty?
                data = {
                    :@type         => "Language",
                    :@id           => Iso639[d].alpha2,
                    :name          => Iso639[d].name,
                    :alternateName => Iso639[d].alpha2,
                }
            end
            data
        }}
    },
    rs_dateline: {
        dateline: { "@" =>  [ lambda { |d,o| 
            [ d["head"]["meta"]["location"] , DateTime.parse(  d["head"]["id"]["_pubdate"] ).strftime("%Y-%m-%d")  ].flatten.reject { |item| item.nil? || item.empty? }.join(', ')

        }] }
    },
    rs_content_location: {
        contentLocation: { "$" =>  lambda { |d,o| 
            rdata = d["location"]
            unless rdata.nil?
                if rdata.kind_of?(String)
                    rdata = {
                        :@type => "Place",
                        :@id => "#{o[:@id]}_PLACE_0", 
                        :name =>rdata ,
                    }

                else
                    rdata =  rdata.each_with_index.map do |l, i|  
                        {
                            :@type => "Place",
                            :@id => "#{o[:@id]}_PLACE_#{i}", 
                            :name => l,
                        }
                    end
                end
            end
            rdata
        } }
    },
    rs_authortagline: {
        creator: { "$.body.['body.end'].tagline.authortagline.p" =>  lambda { |d,o| 
            unless d.nil? || d.empty?
            {
                :@type => "Person",
                :@id => "#{o[:@id]}_PERSON_0",
                :name => d
            }
            end
    }}
    },
    rs_attachments: {
        associatedMedia: { "$.body.['body.head'].attachments.images" =>  lambda { |d,o| 
            rdata = d["_file"]
            rdata =  rdata.values.each_with_index.map do |l, i|  
                {
                    :@type => "ImageObject",
                    :author     => l["credit"],
                    :caption    => l["caption"],
                    :contentUrl => l["_file"],
                    :@id => "#{o[:@id]}_PLACE_#{i}"
                }
            end
            rdata
    }}
    }
    
}
