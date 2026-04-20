#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'language_helpers'

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

            start = Time.now
            rules_ng.run(RULE_SET_v1_0[:rs_record], d, out, o)

            finish = Time.now
            diff = finish - start

            @logger.info ( "Time to apply ruleset to record: #{diff} seconds")

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
            o[:id] = out[:id]

            rules_ng.run(RULE_SET_v1_0[:rs_type], d, out, o)
            o[:type] = out[:type]

            out.clear
            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)
            o[:inLanguage] = out[:inLanguage]
            rdata.merge!(out.data)

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

            rules_ng.run(RULE_SET_v1_0[:rs_url], d, out, o)
            rdata[:url] = out[:url]

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
    rs_record_data: {
        :identifier => {'$.id'=>lambda { |d,o|
            {
                :@type  => "PropertyValue",
                :name   => "Identification of the entity assigned by the provider",
                :@id    => "original_provider_id",
                :value  => d
            }
        }},
        name: { "$" => lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_title_data], d, out, o)
            if out.has_key?(:titleLangAware)
                rdata = out[:titleLangAware]
            else
                if out.has_key?(:title)
                    rdata = out[:title]
                end
            end
            rdata
        }},
        description:{"$.dcDescriptionLangAware" => lambda { |d,o|
            r = []
            d.each { |e|
                l = e[0]  # language code
                n = e[1]  # actual data array
                n.each{ |t|
                    r.append(
                        {
                            :@value =>  t,
                            :@language => detect_language(t, l, o)
                        }
                    )
                }
            }
            r
        }},
        keywords:  [ 
            { "$.edmConceptPrefLabelLangAware" => lambda { |d,o|
                r = []
                d.each { |e|
                    l = e[0]  # language code
                    n = e[1]  # actual data array
                    n.each{ |t|
                        r.append(
                            {
                                :@value =>  t,
                                :@language => detect_language(t, l, o)
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

        sameAs:[ {"$.edmIsShownAt" => lambda { |d,o|
                d
            }}, 
            {'$.guid'=>lambda { |d,o|
                d.split("?").first
            }}
        ],

        associatedMedia:{"$" => lambda { |d,o|
            unless (d["edmIsShownBy"] == nil)
                rdata = {
                    :@type => "MediaObject",
                    #:@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_MEDIAOBJECT_#{0}",
                    :url => d["edmIsShownBy"]
                }
                rdata[:thumbnailUrl] = d["edmPreview"] unless d["edmPreview"] == nil 
                rdata
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
        _aggregator: "$.provider",
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
        ],
        color: { "$.object.aggregations..webResources..edmComponentColor" => lambda { |d,o|
            d
        }}
    },
    rs_title_data: {        
        titleLangAware: { "$.dcTitleLangAware" => lambda { |d,o|   
            # [["en",["ABC","DEF"]],[["de"],["GHI","JKL","MNO"]]] ;  
            # [ "def": [ "Wellcome Exhibition: The History of Pharmacy." ]  ]
                r = []
                d.each { |e|
                    l = e[0]  # language code
                    n = e[1]  # actual data array
                    n.each{ |t|
                        r.append(
                            {
                                :@value =>  t,
                                :@language => detect_language(t, l, o)
                            }
                        )
                    }
                }
                r
        }},
        title: 
            # If there is no property dcTitleLangAware select property title
            { "$.title" => lambda { |d,o|   
                {
                    :@value =>  d,
                    :@language => detect_language(d, "unknown", o)
                }
        }}
        
    },
    rs_language_to_jsonld: {
        data: { "@" => lambda { |d,o|
            r = []
            d = [d] unless d.is_a?(Array)
            d.each { |obj|
                obj.each { |l,v| 
                    v.each{ |e|
                        r.append(
                            {
                                :@value =>  e,
                                :@language => detect_language(e, l, o)
                            }
                        )
                    }
                }
            }
           r
        }}
    }
}