#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

RULE_SET_v0_1 = {
    version: "0.1",
    rs_next_value: {
        search_id: { "$.data.search_id" => [ lambda { |d,o| 
                d
            }]
        },
        cursor: { "$.data.cursor" => [ lambda { |d,o| 
                d
            }]
        },
        has_more: { "$.data.has_more" => [ lambda { |d,o| 
                    d
            }]
        }
    },
    rs_filename:{
        filename: { "$.data" => lambda { |d,o| 
                unless d["videos"].empty?
                    "tiktok_#{d["videos"].first["id"]}_#{d["videos"].last["id"]}.json"
                end
            }
        }
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
        records: { "@" => [ lambda { |d,o| 
            
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
        record: { "$.record" => lambda { |d,o| 

            rdata = {}

            #pp d
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

            if rdata[:inLanguage].nil?
                langcode = rdata["@context"].select{ |e| e.is_a?(Hash) && e.has_key?("@language") }[0]["@language"]
                rdata[:inLanguage] =  {
                    :@type         => "Language",
                    :@id           => Iso639[langcode].alpha2,
                    :name          => Iso639[langcode].name,
                    :alternateName => Iso639[langcode].alpha2
                }
            end

            
            pp o[:config]
            pp o[:config][:additional_dirs][:rosetta_files_dir]
            


            pp rdata[:rosettaLink] 


            rdata

            
        } }
    },
    rs_id:{
        id:  {'$.identifier' =>  lambda { |d,o| 
            if d.is_a?(Hash)
                if d.has_key?("$text")
                    if d["$text"].match(/^http:\/\/abs.lias.be/)
                        d["$text"].gsub('http://abs.lias.be/Query/detail.aspx?ID=','')
                    end
                end
            end
        }}
    },
    
    rs_record_data: {

=begin
root@e1fc652d047f:/source_records/scopeArchiv/fotoalbums_query_0000001/SET1# cut -d '>' -f 1 * | sort -u |grep -v resolver
<dc:date             => datePublished
<dc:description      => description
<dc:format
<dc:identifier
<dc:identifier xsi:type="dcterms:URI"   => identifier
<dc:source           => isPartOf
<dc:title            => name
<dcterms:extent  ?????? 1 album 
<dcterms:isPartOf    => isPartOf

        {"record"=>
            {"title"=>"Photo album of the 150 year jubilee celebrations of the Ursuline congregation of Tildonk",
             "identifier"=>["BE/942855/2277/353", {"$text"=>"http://abs.lias.be/Query/detail.aspx?ID=1656793", "_xsi:type"=>"dcterms:URI"}],
             "extent"=>"1 album",
             "date"=>"1982",
             "source"=>["Archives Ursulines (OSU) - Congregation of Tildonk", "BE/942855"],
             "isPartOf"=>"http://abs.lias.be/Query/detail.aspx?ID=1628195",
             "_xmlns:dc"=>"http://purl.org/dc/elements/1.1/",
             "_xmlns:dcterms"=>"http://purl.org/dc/terms/",
             "_xmlns:xsi"=>"http://www.w3.org/2001/XMLSchema-instance"}}
=end

        description: '$.description',
        name:        '$.title',
        identifier:  {'$.identifier' =>  lambda { |d,o| 
            if d.is_a?(String)
                {
                    :@type => "PropertyValue",
                    :@id   => "scopeArchiv_ref_code",
                    :name  => "scopeArchiv Ref Code",
                    :value => d
                }
            end
        }},
        sameAs:  {'$.identifier' =>  lambda { |d,o| 
            if d.is_a?(Hash)
                if d.has_key?("$text")
                    if d["$text"].match(/^http:\/\/abs.lias.be/)
                        d["$text"]
                    end
                end
            end
        }},
        datePublished: {'$.date' =>  lambda { |d,o| 
            if (d =~ /^[0-9?]{4}$/)
                DateTime.parse("#{d}-1-1").strftime("%Y-%m-%d")
            else
                d
            end
        }},
        isPartOf: { "@" =>  lambda { |d,o| 
            {
                :@type => "Collection",
                :url => d['isPartOf'],
                :name => d['source'].first,
                :@id => d['isPartOf'].split(/\\/).last
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

       #  starts-with => @._resourceIdentifier
        rosettaLink: {'$.source[?(@._resourceIdentifier =~ /^https:\/\/resolver\.libis\.be\/.*/i)]' =>  lambda { |d,o| 
          d['_resourceIdentifier']
        } }
    } 
}
