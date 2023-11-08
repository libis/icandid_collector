#encoding: UTF-8
require 'data_collector'
require "iso639"

BACKLOG_RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(BACKLOG_RULE_SET_v1_0[:rs_data], d["news"], out, o)
                data = out[:data]
                data
        } }
    },
    rs_data: {
        data: { "$.text" => lambda {|d,o|
#            puts      d["id"]
#            puts      d["product"]   
          unless d.empty?
            paper =  d["product"].gsub(/[[:space:]]/, '').downcase
            if o[:papers_to_process].include?(paper)

                date_published = DateTime.parse( d["date"] ).strftime("%Y-%m-%d")
                # date_published =  DateTime.parse( d["date"] ).strftime("%Y-%m-%d")
                rdata = {
                    :datePublished => date_published,
                    :publisher     => {
                        :@type => "Organization",
                        :@id   => I18n.transliterate( paper ).delete(' ').delete('\''),
                        :name  =>  d["product"]
                    },
                }

                INGEST_CONF[:dataset] = {
                    "@id": paper,
                    "@type": "Dataset",
                    "name": d["product"]
                }
                INGEST_CONF[:genericRecordDesc] = "Entry from GoPress - #{ d["product"]  }"

                out = DataCollector::Output.new
                rules_ng.run(BACKLOG_RULE_SET_v1_0[:rs_basic_schema], d, out, o)
                rdata.merge!(out[:basic_schema].to_h)
                o[:@id] = out[:basic_schema].to_h[:@id] 
                out.clear          

                rules_ng.run(BACKLOG_RULE_SET_v1_0[:rs_in_language], d, out, o)
                rules_ng.run(BACKLOG_RULE_SET_v1_0[:rs_text], d, out, o)
                rdata.merge!(out.to_h)
                out.clear

                if rdata[:headline].nil?
                    puts  rdata[:articleBody]
                    rdata[:headline] = rdata[:articleBody].first.to_s.truncate( 150, separator: ' ')
                else
                    rdata[:headline] = rdata[:headline][0]
                end
                rdata[:name] = rdata[:headline]

                if rdata[:articleBody].nil?
                    rdata[:articleBody] = rdata[:headline] unless rdata[:headline].nil?
                else
                    rdata[:articleBody] = rdata[:articleBody].first unless rdata[:articleBody].size != 1
                end

                rdata[:sameAs] = "http://academic.gopress.be/Public/index.php?page=archive-article&issueDate=#{date_published}&articleOriginalId=#{d["id"]}"

                if rdata[:articleBody].empty? && rdata[:headline].empty? 
                    File.open("/records/GoPress/delete_records.js", "a") do |f|
                        f.puts "DELETE /icandid_v2_2/_doc/#{ rdata[:@id ] }" 
                    end

                    #iCANDID_gopress_hetbelangvanlimburg_hetbelangvanlimburgconcentrab08cb9c2-9b87-11ea-be26-2ea77fc8543e22052020-00000
                    rdata[:headline]  = "Record without content"
                    rdata[:name]  = rdata[:headline]
                    rdata[:articleBody] = "Record without content"
                    rdata = { :@id => rdata[:@id ]}

                end
              
                rdata.compact
            end
          end
        } 
      }
    },
    rs_text: {
        description: { "$.lead" => lambda { |d,o|  
            Nokogiri::HTML( d ).text
        } },
        articleBody: { "$.body" => lambda { |d,o|
            Nokogiri::HTML( d ).text
        } },
        headline:  { "$.title" => lambda { |d,o| 
            d
        }},
        printEdition:  { "$.edition" => lambda { |d,o| 
            d.split(/,[\s]*/)
        }},
        articleSection: { "$.section" => lambda { |d,o| 
            d
        }}
    },
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end

            {
                :@id            => "#{ INGEST_CONF[:prefixid] }_#{  INGEST_CONF[:provider][:@id].downcase }_#{ INGEST_CONF[:dataset][:@id].downcase }_#{d["id"]}",
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
    rs_in_language: {
        inLanguage: { "$.langauge" =>  lambda { |d,o| 
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
    }
   
}
