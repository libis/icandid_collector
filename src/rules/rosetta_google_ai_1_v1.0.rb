#encoding: UTF-8
require 'data_collector'
require "iso639"




RULE_SET_GOOGLE_IA_1_v1_0 = {
    version: "1.0",
    rs_records: {
        google_ai_result: { "$" => [ lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_GOOGLE_IA_1_v1_0[:rs_cloud_vision], d, out, o)
            rdata = out[:cloud_vision].clone
            out.clear

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_GOOGLE_IA_1_v1_0[:rs_natural_language], d, out, o)
#            rdata["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"][0]["prov:generated"][0]["prov:wasAttributedTo"] = out[:natural_language]

            unless out[:natural_language].nil?
                rdata["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"] <<  out[:natural_language]["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"][0]
            end

            out.clear
            rdata
        }]}
    },
    rs_wasAttributedTo: {
        wasAttributedTo: { '@' =>  lambda { |d,o| 
            {
                "prov:wasAttributedTo" => [
                    {
                        "@type" => [ "prov:Agent", "agent"],
                        "name" => "Google Cloud",
                        "prov:Agent" => "Google Cloud",
                        "@id" => "googlecloud",
                        "url" => "https://cloud.google.com/"
                    }
                ]
            }
        }}
    },
    rs_cloud_vision: {        
        cloud_vision: { "$.texts[0].description" => [ lambda { |d,o| 

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_GOOGLE_IA_1_v1_0[:rs_wasAttributedTo], d, out, o)
            rdata = out[:wasAttributedTo]
            rdata["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"] = [
                    {
                    "@type" => [
                        "prov:Activity",
                        "action"
                    ],
                    "name" => "Cloud Vision API",
                    "@id" => "google_ai_vision_image_text_extraxtion",
                    "prov:generated" => [
                            {
                                "additionalType" => "CreativeWork",
                                "@type" => "DigitalDocument",
                                "name" => {
                                    "@value" => "Text extraction from img",
                                    "@language" => "nl_latn"
                                },
                                "text" => d
                            }
                        ]
                    }
                ]
            rdata
       } ] }
    },
    rs_natural_language: {        
        natural_language: { "$" => [ lambda { |d,o| 

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_GOOGLE_IA_1_v1_0[:rs_wasAttributedTo], d, out, o)
            rdata = out[:wasAttributedTo]
            out.clear

            rules_ng.run(RULE_SET_GOOGLE_IA_1_v1_0[:rs_entities], d, out, o)
            entities = out[:entities]

            if entities.nil?
                rdata = nil
            else
                _gen_entites = {}
                entities.each { |e|
                    _gen_entites["ALL"] = e["name"]["@value"]
                    _gen_entites[e["additionalType"]] = e["name"]["@value"]
                }
                rdata["prov:wasAttributedTo"][0]["prov:wasAssociatedFor"] = [
                    {
                        "@type" => [
                            "prov:Activity",
                            "action"
                        ],
                        "name": "Cloud Natural Language API",
                        "@id": "google_ai_nlp_ner_en",
                        "prov:generated": entities,
                        "_generated": _gen_entites
                    }
                ]
            end    
            rdata
       } ] }
    },
    rs_entities: {        
        entities: { "$.entities" => [ lambda { |d,o| 

            additionalType = case d['type']
            when "DATE"
              "DATE"
            when "NUMBER"
                nil
            else
                pp d
                nil
            end 

            unless additionalType.nil?
                rdata = {
                    "additionalType" => additionalType,
                    "name" => {
                    "@value" => d['name'],
                    "@language" => "nl-Latn"
                    }
                }
            end
            rdata
        } ] }
    }


}
