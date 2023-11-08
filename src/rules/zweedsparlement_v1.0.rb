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
            rdata = {
                :name            => "#{d["titel"]}, #{d["undertitel"]}",
                :description     => d["summary"],
                :datePublished   => d["publicerad"],
                :legislationType => [d["typ"],d["subtyp"]],
                #:sameAs          => "https://data.riksdagen.se/dokumentstatus/#{d["id"]}.json",  
                :sameAs          => d["dokument_url_html"],
                :publisher       => o[:default_publisher],
                :identifier      => [
                    { "@type"=> "PropertyValue", :@id => "data_riksdagen_source_id_#{d["kall_id"]}", :name => "data_riksdagen_source_id", :value => d["kall_id"] },
                    { "@type"=> "PropertyValue", :@id => "data_riksdagen_doc_id_#{d["dok_id"]}", :name => "data_riksdagen_doc_id", :url => d["dokument_url_text"], :value => d["dok_id"] }
                ]

            }

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_legislationPassedBy], d["organ"], out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_legislationType], d, out, o)
            rules_ng.run(RULE_SET_v1_0[:rs_text], d["filbilaga"], out, o)

            o[:index] = 0

            rules_ng.run(RULE_SET_v1_0[:rs_contacttype], d["dokintressent"], out, o)
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
    rs_legislationPassedBy: {
        legislationPassedBy: { "$" =>  lambda { |d,o| 
            organ = o[:organ].select{ |org| d == org["kod"] }
            organ.map{ |org|
                {
                    :@type => "Organisation",
                    :@id   => "#{o[:prefixid]}_ORGANISATION_#{ org["kod"]  }",
                    :name  => org["namn"],
                    :alternateName  => org["namn_en"],
                    :description  => org["beskrivning"]
                }
            }
        }}
    },
    rs_legislationType: {
        legislationType: { "$" =>  lambda { |d,o| 
            o[:doktyp].select{ |dt|
                d["doktyp"] == dt["doktyp"] &&
                d["typ"] == dt["typ"] &&
                (d["subtyp"] == dt["subtyp"] ||  dt["subtyp"] == "" ||  dt["subtyp"] == "-")
            }.map!{ |dt| dt["namn"]}.uniq
        }}
    },
    rs_text: {
        text: { "@" => lambda { |d,o| 
            d["fil"].select{ |fil| fil["typ"] == "pdf" }
                    .map{ |fil| fil["namn"] }
        }}
    },
    rs_in_language: {
        inLanguage: { "$" =>  lambda { |d,o| 
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end
            
            unless Iso639[language].nil? || Iso639[language].alpha2.to_s.empty?
                {
                    :@type         => "Language",
                    :@id           => Iso639[language].alpha2,
                    :name          => Iso639[language].name,
                    :alternateName => Iso639[language].alpha2,
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
        legislationResponsible: {  "$.intressent[?(@.roll == 'undertecknare')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_contact],  d, out, o)
            out[:contact]
        }},
        author: { "$.intressent[?(@.roll == 'undertecknare')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_contact],  d, out, o)
            out[:contact]
        }}
    }, 
    rs_contact: {
        contact: { "$" =>  lambda { |d,o|
            if d["intressent_id"]
                rdata = {
                    :@type => "Person",
                    :@id   => "#{o[:prefixid]}_PERSON_id_#{ d["intressent_id"] }",
                    :name  => "#{d["namn"]}"
                }
            else    
                rdata = {
                    :@type => "Organisation",
                    :@id   => "#{o[:@id]}_ORGANISATION_#{ o[:index] }",
                    :name  => d["namn"]
                }
            end  

            if d["partibet"] 
                partibet = o[:organ].select { |org| org["kod"] == d["partibet"] }.first
                unless partibet.nil?
                    rdata["memberOf"] = {
                        :@type => "Organisation",
                        :@id   => "#{o[:prefixid]}_ORGANISATION__id_parti_#{ d["partibet"]  }",
                        :name  => partibet["namn"],
                        :alternateName  => partibet["namn_en"],
                        :description  => partibet["beskrivning"]
                    }
                end
            end
            rdata
        }}
    }

}
