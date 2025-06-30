#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
include ActiveSupport::Inflector 

DEBUG = true

RULE_SET_v1_0 = {
    version: "1.0",
    rs_next_value: {
        page: { "$" => lambda { |d,o| 
            o["query"]["params"]["page"]+1
        }}
    },
    rs_filename:{
        filename: { "@" => lambda { |d,o| 
           "#{d.first["date"].to_date.strftime('%Y%m%d') }_#{d.first["id"]}_#{d.last["id"]}.json" 
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
        records: { "@" => lambda { |d,o|  
            pp "rules_ng.run(RULE_SET_v1_0[:rs_record] .. )" if DEBUG
            records = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_record], d, records, o)
            records[:record]
        } }
    },
    rs_record: {
        record: { "$.data" => lambda { |d,o| 
            if d["text"] =~ /{"detail":"No session found with id .*"}/
                return nil
            end

            rdata = {}
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

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
    rs_record_data: {
        name:    {'$.date' =>  lambda { |d,o| 
            date = Time.parse(d)
            "Session of #{ date.strftime("%B #{ordinalize(date.day)}, %Y") }"
        }},
        text: '$.text',
        sameAs:  {'$.id' =>  lambda { |d,o| 
            "https://plenum.be/api/sessions/#{d}"
        }},
        datePublished: {'$.date' =>  lambda { |d,o| 
            unless d.nil?
                Time.parse(d).strftime("%Y-%m-%d")
            end
        }},
        inLanguage: { "$" =>  lambda { |d,o|    
            rdata = []
            l = "nl"
            rdata << 
                {
                    :@type         => "Language",
                    :@id           => Iso639[l].alpha2,
                    :name          => Iso639[l].name,
                    :alternateName => Iso639[l].alpha2,
                }
            l = "fr"
            rdata << 
                {
                    :@type         => "Language",
                    :@id           => Iso639[l].alpha2,
                    :name          => Iso639[l].name,
                    :alternateName => Iso639[l].alpha2,
                }                
            rdata
        }},
        associatedMedia: {'$.sameAs' =>  lambda { |d,o| 
            {
                :@type => "MediaObject",
                :url => d
            }
        }},
    } 
}
