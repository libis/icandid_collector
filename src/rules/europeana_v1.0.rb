#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'detect_language_script'

RULE_SET_v1_0 = {
    version: "0.1",
    rs_next_value: {
    },
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
                unless d["id"].empty?
                    "europeana_#{d["id"]}.json"
                end
        } }
    },
    rs_raw_data:{
        data: { "$" => lambda { |d,o| 
               d
            }
        },
        error: { "$" => lambda { |d,o| 
            nil
            }
        }
    },
    rs_records: {
        records: { "$." => [ lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_record], d, out, o)

            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
            
            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$" => lambda { |d,o| 

            rdata = {}

            #pp d
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_v1_0[:rs_type], d, out, o)
            o[:type] = out[:type].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)

            rdata.merge!(out.data)

            rules_ng.run(RULE_SET_v1_0[:rs_url], d, out, o)
            rdata[:url] = out[:url].first

            if rdata[:inLanguage].nil?
                langcode = rdata["@context"]["@language"]
                rdata[:inLanguage] =  {
                    :@type         => "Language",
                    :@id           => Iso639[langcode].alpha2,
                    :name          => Iso639[langcode].name,
                    :alternateName => Iso639[langcode].alpha2
                }
            end
            rdata

            
        } }
    },
    rs_id:{
        id:  {'$.id' =>  lambda { |d,o| 
            d
        }}
    },
    rs_type: {
        type: {'$.type' => lambda { |d,o|
            o[:types][d]
        }}
    },
    rs_url: {
        url: {'$.guid'=>lambda { |d,o|
            d.split("?").first
        }}
    },
    rs_record_data: {
        :identifier => {'$.guid'=>lambda { |d,o|
            {
                :@type  => "PropertyValue",
                :name   => "Identification of the entity assigned by the provider",
                :@id    => "orignal_provider_id",
                :value  => d.split("?").first
            }
        }},
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
        }},
        name:{ "$.dcTitleLangAware" => lambda { |d,o|   # [["en",["ABC","DEF"]],[["de"],["GHI","JKL","MNO"]]]
            out = DataCollector::Output.new
            r = []
            d.each { |e|
                l = e[0]  # language code
                n = e[1]  # actual data array
                n.each{ |t|
                    rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], t, out, o)
                    r.append(
                        {
                            :@value =>  t,
                            :@language => "#{l.downcase}-#{out[:detect_language_script][0]}"
                        }
                    )
                }
            }
            r
        }},
        _name: {"$.object.proxies..dcTitle" => lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_language_to_jsonld], d, out, o)
            out[:data]
        }},
        description:{"$.dcDescriptionLangAware" => lambda { |d,o|
            out = DataCollector::Output.new
            r = []
            d.each { |e|
                l = e[0]  # language code
                n = e[1]  # actual data array
                n.each{ |t|
                    rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], t, out, o)
                    r.append(
                        {
                            :@value =>  t,
                            :@language => "#{l.downcase}-#{out[:detect_language_script][0]}"
                        }
                    )
                }
            }
            r
        }},
        keywords:  [ 
            { "$.edmConceptPrefLabelLangAware" => lambda { |d,o|
                out = DataCollector::Output.new
                r = []
                d.each { |e|
                    l = e[0]  # language code
                    n = e[1]  # actual data array
                    n.each{ |t|
                        rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], t, out, o)
                        r.append(
                            {
                                :@value =>  t,
                                :@language => "#{l.downcase}-#{out[:detect_language_script][0]}"
                            }
                        )
                    }
                }
                r
            }},
            { "$.object.proxies..dcSubject" => lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_0[:rs_language_to_jsonld], d, out, o)
                out[:data]
            }}
        ],
        creator:{"$.dcCreatorLangAware" => lambda { |d,o|
            out = DataCollector::Output.new
            r = []
            d.each { |e|
                l = e[0]  # language code
                n = e[1]  # actual data array
                n.each{ |t|
                    r.append(
                        {
                            :@type => "Person",
                            #:@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_PERSON_#{0}",
                            :name => t 
                        }
                    )
                }
            }
            r
        }},
        contentLocation:{"$.currentLocation" => lambda { |d,o| 
            {
                :@type => "Place",
                #:@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_PLACE_#{0}",
                :name => d
            }
        }},
        sameAs:{"$.edmIsShownAt" => lambda { |d,o|
            d
        }},
        associatedMedia:{"$" => lambda { |d,o|
            unless (d["edmIsShownBy"] == nil && d["edmPreview"] == nil )
                {
                    :@type => "MediaObject",
                    #:@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_MEDIAOBJECT_#{0}",
                    :url => d["edmIsShownBy"],
                    :thumbnailUrl => d["edmPreview"]
                }
            end
        }},
        temporalCoverage:{"$.edmTimespanLabel" => lambda { |d,o|
            d["def"]
        }},
        publisher:{"$.dataProvider" => lambda { |d,o|
            {
                :@type => "Organization",
                :name  => d
            }
        }},
        isPartOf: [ 
            { "$.object.organizations" => lambda { |d,o|
                rdata = {
                    :@type => "Collection",
                    :@id =>  d["about"]
                }  
                if d["prefLabel"].has_key?("en")
                    rdata[:name] = {
                        :@value => d["prefLabel"]["en"],
                        :@language => 'en-Latn'
                    }
                end
                if d["prefLabel"].has_key?("fr")
                    rdata[:name] = {
                        :@value => d["prefLabel"]["fr"],
                        :@language => 'fr-Latn'
                    }
                end
                if d["prefLabel"].has_key?("nl")
                    rdata[:name] = {
                        :@value => d["prefLabel"]["nl"],
                        :@language => 'nl-Latn'
                    }
                end
                rdata
            }}
        ],
        license:  [
            { "$.rights" => lambda { |d,o|
                d
            }},
            { "$.object.aggregations..edmRights.def" => lambda { |d,o|
                d
            }}
        ],
        copyrightNotice:[
            { "$.object.proxies..dcRights" => lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_0[:rs_language_to_jsonld], d, out, o)
                out[:data]
            }}
        ]
    },
    rs_language_to_jsonld: {
        data: { "@" => lambda { |d,o|
            out = DataCollector::Output.new
            r = []
            d = [d] unless d.is_a?(Array)
            d.each { |obj|
                obj.each { |k,v| 
                    lang = k ==="def" ? "en" : k
                    v.each{ |e|
                        rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], e, out, o)
                        r.append(
                            {
                                :@value =>  e,
                                :@language => "#{lang.downcase}-#{out[:detect_language_script][0]}"
                            }
                        )
                    }
                }
            }
           r
        }}
    }
}