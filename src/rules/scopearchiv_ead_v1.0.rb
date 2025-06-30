#encoding: UTF-8

# De metadata van ScopeArchiv wordt aangevuld met gegevens uit Rosetta.
# Zie associatedMedia voor meer info

require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'language_helpers'


Dir[  File.join( ROOT_PATH,"src/rules/rosetta_*.rb") ].each {|file| require file; }

ROSETTA_RULES = "ROSETTA_IIIF_RULES_1_0".constantize 

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "@" => [ lambda { |d,o| 
            
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_record], d, out, o)

            if out[:record].nil?
                pp d.keys
                pp "ERROR: No data in record !"
                pp out
            end

            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$.ead" => lambda { |d,o| 

            rdata = {}
            o[:translate_language] = { :input_language => "nl", :output_language => "en" }

            icandid_input  = IcandidCollector::Input.new()
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_metadata_language], d, out, o)
            o[:ingest_data][:metaLanguage] =  out.data["metadata_language"].first

            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)

            unless out.data[:associatedMedia].nil?
                url = out.data[:associatedMedia][:embedUrl].gsub('representation','metadata')

                # pp out.data[:identifier]

                data = icandid_input.collect_data_from_uri(url:  url  ,  options: o )
                out.data[:identifier] =  [ out.data[:identifier] ] unless  out.data[:identifier].is_a?(Array)

                data["identifier"] = [ data["identifier"]  ] unless data["identifier"].is_a?(Array)
                
                out.data[:identifier].concat ( 
                    data["identifier"]
                        .select {|i| i.is_a?(String) && /.tif$/ =~ i }
                        &.map { |i| 
                        # pp i
                        {
                            :@type => "PropertyValue",
                            :@id   => "teneo_id",
                            :name  => "Teneo ID",
                            :value => i.gsub('.tif','')
                        }
                    })

                unless out.data[:identifier].select{ |i|  i[:name] == "Teneo ID" }.empty?
                    out.data[:identifier] = out.data[:identifier].reject{ |i| i[:name] == "Teneo ID [Extracted From Repositorycode]" }
                end

            end
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
        id:  '$.eadheader.eadid._identifier'
    },
    rs_metadata_language:{
        metadata_language: {'$.eadheader.profiledesc.langusage.language.$text' =>  lambda { |d,o| 
            out = DataCollector::Output.new 
            rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_translate_language], d, out, o)
            out[:translate_language_name_to_code].first
        }}
    },
    rs_record_data: {
        
        identifier:  {'$.archdesc.descgrp..unitid._repositorycode' =>  lambda { |d,o| 
            if d.is_a?(String)
                rdata = {
                    :@type => "PropertyValue",
                    :@id   => "scopeArchiv_ref_code",
                    :name  => "scopeArchiv Ref Code",
                    :value => d
                }

                letters, numbers = d.split('/').last.downcase.match(/(^[a-z]*)([0-9]*)/).captures
                id = "#{letters}#{numbers.rjust(6, '0')}"

                unless Dir["#{o[:config][:query][:googeAI_test_dir]}*#{id}*.json"].empty?
                    pp  "#{o[:config][:query][:googeAI_test_dir]}*#{id}*.json"
                    rdata = [
                        rdata = {
                            :@type => "PropertyValue",
                            :@id   => "scopeArchiv_ref_code",
                            :name  => "scopeArchiv Ref Code",
                            :value => d
                        },
                        {
                            :@type => "PropertyValue",
                            :@id   => "teneo_id",
                            :name  => "Teneo ID [Extracted From Repositorycode]",
                            :value => "#{id}"
                        }
                    ]
                    
                end

                rdata
                
               

            end
        }},

        holdingArchive: { '$.archdesc.descgrp.repository.corpname'  =>  lambda { |d,o| 
            {
                :@type => "ArchiveOrganization",
                :@id   => "KADOC",
                :name  => d
            }
        }},
        creator: { '$.archdesc.descgrp.origination[?(@._label=="Archiefvormer")]..p'  =>  lambda { |d,o| 
            {
                :@type => "Person",
                :name  => d
            }
        }},
        name:   { '$.archdesc.descgrp..unittitle..p'=>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], d, out, o)
            {
                :@value => d,
                :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
            }
        }}, 
        datePublished: '$.archdesc.descgrp..unitdate..p',
        inLanguage:    {'$.archdesc.descgrp..langmaterial..p' =>  lambda { |d,o|

            out = DataCollector::Output.new
            
            rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_translate_language], d, out, o)
            d = out[:translate_language_name_to_code].first

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
        material:  [ '$.archdesc.descgrp..genreform..p', '$.archdesc.descgrp..phystech..p'  ],
        materialExtent: '$.archdesc.descgrp..extent..p',
        # acquiredFrom: '$.archdesc.descgrp..custodhist..p',        => Only 1520249.xml contains this tag 
        keywords: '$.archdesc.controlaccess..controlaccess.extref.persname.$text',
        sameAs:   '$.eadheader.daoset.dao[?(@._daotype=="otherdaotype")]._href',
        description: [{'$.archdesc.descgrp..note..p' =>  lambda { |d,o| 
            out = DataCollector::Output.new
                rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], d, out, o)
                {
                    :@value => d,
                    :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
                }
            }},
            {'$.archdesc.descgrp..scopecontent..p' =>  lambda { |d,o| 
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], d, out, o)
                {
                    :@value => d,
                    :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
                }
            }}
        ],
=begin
        about: {'$.archdesc.descgrp..scopecontent..p' =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], d, out, o)
            {
                :@value => d,
                :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
            }
        }},
=end
        associatedMedia: {  '$.eadheader.daoset.dao[?(@._daotype=="derived")]._href' =>  lambda { |d,o|
            {
                :@type => "MediaObject",
                :contentUrl => d
            }     
        }}
           
    } 
}
