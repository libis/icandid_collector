#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

ROSETTA_IIIF_RULES_1_0 = {
    version: "1.0",
    rs_records: {
        records: { "@" => [ lambda { |d,o| 
            rdata = []

            o[:ingest_data] = {
                "provider": { 
                    "@id":  "KADOC_teneo",
                    "@type": "Organization",
                    "name": "KADOC Teneo",
                    "alternateName": "KADOC_teneo"
                },
                "dataset": {
                    "@id": "KADOC_rosetta_dataset",
                    "@type": "Dataset",
                    "name":  "KADOC Dataset",
                    "license": "https://creativecommons.org/licenses/"
                },
                "same_as_template": "",
                "mediaUrlPrefix": "",
                "metaLanguage": "nl",
                "unicode_script": "Latn",
                "recordLanguage": "nl",
                "genericRecordDesc": "Entry from Teneo KADOC",
                "prefixid"=>"iCANDID"
            }

            out = DataCollector::Output.new
            rules_ng.run(ROSETTA_IIIF_RULES_1_0[:rs_record], d, out, o)


            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
            
            rdata.insert(0,out[:record])

            rdata


        } ] }
    },
    rs_record: {
        record: { "@" => lambda { |d,o| 
            rdata = {
                sameAs:       d['@id'],
                playertype:   "IIIF Viewers",
                name:         d['label'], 
                thumbnailUrl: "https://resolver.libis.be/#{o[:id]}",
                embedUrl:     "https://resolver.libis.be/#{o[:id]}/representation"
            }

            o[:type] = "MediaObject"

            # idee om de gegevens uit de bestanden van de rosetta export te halen ?
            # pp o[:config]
            # pp o[:config][:additional_dirs][:rosetta_files_dir]
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            rdata["isBasedOn"].delete("isPartOf")
            out.clear

            if d['sequences'].size > 1
                pp "more than 1 sequences for #{o[:id]}"
                exit
            end

            rules_ng.run(ROSETTA_IIIF_RULES_1_0[:rs_record_data], d, out, o)
            
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
            rdata.delete("@context")

            rdata
       } }
    },
    rs_id:{
        id: '$.@id'
    }, 
    rs_record_data:{
        hasPart: { '$.sequences'  =>  lambda { |d,o| 

            out = DataCollector::Output.new
            rules_ng.run(ROSETTA_IIIF_RULES_1_0[:rs_sequences], d, out, o)
            # out.data
            out.data[:hasPart].map{ |p| p[:name] = out.data[:name]; p  }


        }}
    },
    rs_sequences:{
        url:  {'$.@id' =>  lambda { |d,o| d }},
        "@type": {'text' => "Collection"},
        name:  '$.label',
        thumbnailUrl:  '$.thumbnail',
        hasPart: { '$.canvases'  =>  lambda { |d,o| 

            if d['images'].size > 1
                pp "more than 1 image for #{o[:id]}"
                exit
            end

            out = DataCollector::Output.new
            rules_ng.run(ROSETTA_IIIF_RULES_1_0[:rs_canvases], d, out, o)
            out.data
           
        }}
    },
    rs_canvases:{
        "@id": {'$.@id' =>  lambda { |d,o| 
            "#{o[:ingest_data][:prefixid]}_#{o[:ingest_data][:dataset][:@id]}_#{o[:id]}_#{ d.split('/').last }"
        }},
        url:  '$.@id',
        "@type": {'text' => "MediaObject"},
        identifier: { '$.label' => lambda { |d,o| 
            {
                "@type": "PropertyValue",
                "@id": "teneo_canvas_id_#{d}",
                name: "teneo_canvas_id",
                value: "#{d}"
            }
        } },
        thumbnailUrl:  '$.thumbnail',
        # associatedMedia: { '$.images'  =>  lambda { |d,o| d }}

    }
}