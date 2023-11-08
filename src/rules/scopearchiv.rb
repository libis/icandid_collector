#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "@" => [ lambda { |d,o| 
            rdata = []
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_record], d, out, o)

            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end

            unless out[:record][:hasPart].nil?
                # Make seperated records from the hasPart objects
                rdata.concat( out[:record][:hasPart].clone )
                # Only keep the properties :@id, :@type, :url, :name, :desc in :hasPart in the overall record
                out[:record][:hasPart].map! { |m| m.slice(:@id, :@type, :url, :name, :desc) }
            end

            rdata.insert(0,out[:record])

            rdata

        } ] }
    },
    rs_record: {
        record: { "$.mets" => lambda { |d,o| 

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data_array], d, out, o)
            rdata = out[:data]
            out.clear

            o[:datePublished] = rdata[:datePublished]

            rules_ng.run(RULE_SET_v1_0[:rs_data_has_part], d['structMap'], out, o)
            rdata.merge!(out.to_h)
            out.clear

            rdata[:hasPart] = [ rdata[:hasPart] ] if rdata[:hasPart].is_a?(Hash) 

            # Expand the data from hasPart with info from d['amdSec']
            rdata[:hasPart].map! { |p|
                id = p.delete(:_FILEID)
                input = d['amdSec'].select { |s| s['_ID'] == "#{id}-amd" }
                unless input.size != 1
                    rules_ng.run(RULE_SET_v1_0[:rs_datapart_expand], input[0], out, o)
                    p.merge!(out[:datapart_expand])

                    p[:name] =p[:name].first
                    out.clear
                end
                p
            }

            rdata
        } }
    },

    rs_data_array: {
        data: { "@.dmdSec[?(@._ID=='ie-dmd')]" => [ lambda {|d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data], d['mdWrap']['xmlData']["record"], out, o)
            out[:data]
        } ]}
    },
    rs_data: {
        data: { "@" => [ lambda { |d,o|
            rdata = {
                :sameAs => "http://resolver.libis.be/#{ o[:id_from_path] }/representation"
            }

            unless  d['type'].nil?
                type = d['type'].is_a?(Array) ? d['type'] : [ d['type'] ]  
                type.map!{ |t| t.is_a?(Hash) && t.has_key?('$text') ? t['$text'] : t }
                type.select!{ |s| s.match(/journal/) }
                unless type.empty?
                    o[:type] = "PublicationIssue"
                end                    
            end
=begin
                    rdata[:text] = "https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?fulltext=true&dps_pid=#{o[:id_from_path]}" 
                    rdata[:'prov:wasAttributedTo'] = [
                        {
                            :@id => "Rosetta",
                            :@type => ["prov:Agent", "agent"],  
                            :'prov:SoftwareAgent' => "FullTextExtractor",
                            :url => "https://exlibrisgroup.com/products/rosetta-digital-asset-management-and-preservation/",
                            :name => "Full text extration with Rosetta Plugin",
                            :'prov:wasAssociatedFor' => [{
                                :@id => "ROSETTA_FT_EXTRACTION",
                                :name =>  "",
                                :@type => ["prov:Activity" , "action"],
                                :'prov:used' => {
                                    :@id => "ROSETTA_TIKA",
                                    :name => "Rosetta tika"
                                },
                                :'prov:generated' => [
                                    {
                                        :@type => "DigitalDocument",
                                        :additionalType => "CreativeWork",
                                        :name => {
                                                :@value => "Full Text extracted from PDF [#{ o[:id_from_path] }]",
                                                :@language =>"nl_latn"								   
                                        },
                                        :text => "https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?fulltext=true&dps_pid=#{o[:id_from_path]}" 
                                    }
                                ]
                            }]
                        }    
                    ]
=end                    
    
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_record_data], d, out, o)

            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)
            
            rules_ng.run(RULE_SET_v1_0[:rs_is_part_of], d, out, o)

            rdata.merge!(out.to_h)

            rdata.compact
        } ] }
    },
    rs_data_has_part: {
        hasPart: { "$" => [ lambda {|d,o|
            o[:desc] = d['div']['_LABEL']
            o[:name] = d['div']['div']['_LABEL']
            # pp o[:desc]
            # pp o[:name]
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data_part], d['div']['div']['div'] , out, o)
            out[:part]
        } ] } 
    },
    rs_data_part: {
        part: { "$" => [ lambda {|d,o|
            rdata = {
                :datePublished  => o[:datePublished],
                :name     => o[:name],
                :desc     => "#{o[:desc]} : #{d['_LABEL']}",
                :url      => "#{INGEST_CONF[:url_prefix] }#/record/#{o[:prefixid]}_#{o[:id_from_path]}_#{ d['fptr']['_FILEID']}" ,
                :_FILEID  => d['fptr']['_FILEID'],
                :sameAs   => "https://resolver.libis.be/#{ d['fptr']['_FILEID'] }/wstream",
                :isPartOf => {
                    :@id      => "#{o[:prefixid]}_#{o[:id_from_path]}",
                    :url      => "#{INGEST_CONF[:url_prefix] }#/record/#{o[:prefixid]}_#{o[:id_from_path]}"
                },
                :identifier => {
                    :@type => "PropertyValue",
                    :@id   => "teneo_file_id_#{d['fptr']['_FILEID']}",
                    :name  => "teneo_file_id",
                    :value => d['fptr']['_FILEID']
                }
            }

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rdata[:@id] = "#{o[:prefixid]}_#{o[:id_from_path]}_#{ d['fptr']['_FILEID']}"
            rdata[:@type] ="CreativeWork"
            rdata
        } ] }
    },
    rs_datapart_expand: {
        datapart_expand: { "$.techMD.mdWrap.xmlData.dnx" => [ lambda {|d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_dataparts_section], d, out, o)
            if ( [ "image/jpeg", "image/tiff", "image/jp2" ] & out[:encodingFormat] ).size > 0
                out[:@type]  = "ImageObject"
            end
            if  ( [ "application/pdf", "application/txt" ] & out[:encodingFormat] ).size > 0
                out[:@type] = "DigitalDocument"
            end
            out.to_h
        } ] }
    },
    rs_dataparts_section: {
        exifData: { "$.section[?(@._id=='significantProperties')]" => [ lambda {|d,o|
            properties = ["focalPlaneResolutionUnit","bitsPerSample","samplesPerPixel","imageWidth","imageLength","compressionType","compressionScheme","flashpixVersion","colourSpace"]
            section = []
            d["record"].each { |r| 
                type = r["key"].select { |s| s.is_a?(Hash) && s["_id"] == "significantPropertiesType" }.map{ |m| m["$text"]}.first
                value = r["key"].select { |s| s.is_a?(Hash) && s["_id"] == "significantPropertiesValue" }.map{ |m| m["$text"]}.first
                if value.nil? || value.empty?
                    if (r["key"][1].is_a?(TrueClass) || r["key"][1].is_a?(FalseClass))
                        value = r["key"][1]
                    end                   
                end
                type = type.split('.').last

                if properties.include?(type)
                    unless value.nil?
                        section << 
                            { 
                                :@type => "PropertyValue",
                                :name => type,
                                :value => value
                            }
                    end
                end
            }
            section
        } ] },
        encodingFormat: [ 
            "$.section[?(@._id=='generalFileCharacteristics')].record.key[?(@._id=='fileMIMEType')].$text",
            "$.section[?(@._id=='significantProperties')].record.key[?(@._id=='mimeType')].$text"
        ],
        name: "$.section[?(@._id=='generalFileCharacteristics')].record.key[?(@._id=='label')].$text"
    },    
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end

            {
                :@id            => "#{o[:prefixid]}_#{o[:id_from_path]}",
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
    rs_record_data: {
        :description     => '$.description',
        :name            => '$.title',
        :identifier      => {'$.identifier' =>  lambda { |d,o| 
                                if d.is_a?(String)
                                    {
                                        :@type => "PropertyValue",
                                        :@id   => "teneo_id_#{d}",
                                        :name  => "teneo_id",
                                        :value => d
                                    }
                                end
        }},
        :alternateName   => '$.alternative',
        :keywords        => {'$.subject' =>  lambda { |d,o|  d['$text']} }, 
        :startDate       => {'$.publisher' =>  lambda { |d,o| d.split(/-/)[0] } },
        :endDate         => {'$.publisher' =>  lambda { |d,o| d.split(/-/)[1] } },
        :printEdition    => '$.coverage',
        :spatialCoverage => '$.coverage',
        :datePublished   => {'$.date' =>  lambda { |d,o| 
            if (d =~ /^[0-9?]{4}$/)
                DateTime.parse("#{d}-1-1").strftime("%Y-%m-%d")
            else
                d
            end
        }},
        creator: { "$.creator" =>  lambda { |d,o| 
            rdata = {
                :@type => "Person",
                :@id   => "#{o[:prefixid]}_PERSON_#{ o[:index] }",
                :name  => d
            }
            o[:index] =  o[:index]+1
            rdata
        }}      

        # 'issueNumber' = Nog uit de titlle te halen ?
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
    rs_is_part_of: {
        isPartOf: { "$.isPartOf" =>  lambda { |d,o| 
            {
                :@type => "Periodical",
                :name => d,
                :@id => d.split(/ /).last
            }
        }}
    },
    rs_collection: {
        is_part_of_collection: { "@.amdSec[?(@._ID == 'ie-amd')].techMD.mdWrap.xmlData.dnx.section" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_collection_section], d, out, o)
            out[:collection]
        }}
    },
    rs_collection_section: {
        collection: { "@.record.key[?(@._id == 'collectionId')]" =>  lambda { |d,o| 
            {
                :@type => "Collection",
                :url => "https://repository.teneo.libis.be/delivery/action/collectionViewer.do?collectionId=#{d['$text']}&operation=viewCollection&displayType=list",
                :@id => "#{o[:prefixid]}_COLLECTION_#{d['$text']}"
            }
        }}
    }
 
    # 
    # https://repository.teneo.libis.be/delivery/action/collectionViewer.do?collectionId=264401606&operation=viewCollection
    
}

=begin

'publisher'  => '$.field[?(@._name=="publisher")].text',
'inLanguage'  => '$.field[?(@._name=="language")].text',

'  => '$.field[?(@._name=="date")].text',
'            => '$.field[?(@._name=="type")].text',
'  => '$.field[?(@._name=="accrualPeriodicity")].text',
'  => '$.field[?(@._name=="format")].text',
'  => '$.field[?(@._name=="coverage")].text',
'  => '$.field[?(@._name=="isPartOf")].text',
=end
