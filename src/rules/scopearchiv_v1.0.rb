#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

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
                pp "MAYDAY_MAYDAY"
                pp out
            end

            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$.record" => lambda { |d,o| 

            rdata = {}

            # idee om de gegevens uit de bestanden van de rosetta export te halen ?
            # pp o[:config]
            # pp o[:config][:additional_dirs][:rosetta_files_dir]

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
=begin
            pp "ssssssssssssssssssssssss - scopeArchive rule output - ssssssssssssssssssssssss"
            
            pp rdata.keys
            pp rdata[:isPartOf]
            pp rdata[:associatedMedia].size
            pp rdata[:associatedMedia].map { |a| 
                {
                    type: a["associatedMedia"]["@type"],
                    name: a["associatedMedia"]["name"],
                    haspart: a["associatedMedia"]["hasPart"].map { |aa|
                    {
                        type: aa["@type"],
                        url: aa["url"]
                    }
                }
            }
            }
            pp "sssssssssssssssssssssssssssssssssssssssssssssssssssssssssssssss"
            =end    
        

=begin            
                { 
                    id: a["@id"],
                    type: a["@type"],
                    associatedMedia: {
                        name: a["associatedMedia"][:name],
                        type: a["@type"],
                        url: a["associatedMedia"][:url],
                        hasPart: a["associatedMedia"][:hasPart].map { |aa|
                            {
                                type: aa["@type"],
                                url: aa["url"]
                            }
                        }
                    }
                }
=end                


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
                :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{d['isPartOf'].split('=').last}"
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
       associatedMedia: {'$.source[?(@._resourceIdentifier =~ /^https:\/\/resolver\.libis\.be\/.*/i)]' =>  lambda { |d,o| 
            # Metadata from intellectual entity (example is: IE13673061 IE13097737 )
            # https://resolver.libis.be/IE13673061/metadata => Rosetta data
            # https://resolver.libis.be/IE13097737/metadata => Rosetta data
            # https://repository.teneo.libis.be/oaiprovider/request?verb=GetRecord&identifier=oai:teneo.libis.be:IE13673061&metadataPrefix=oai_dc
            #   => idDoesNotExist ? Not all Rosetta data is available through oai pmh
            # https://repository.teneo.libis.be/oaiprovider/request?verb=GetRecord&identifier=oai:teneo.libis.be:IE13097737&metadataPrefix=oai_dc
            # => idDoesNotExist ? Not all Rosetta data is available through oai pmh
            # https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?dps_pid=IE13673061&manifest=true => IIIF format
            # https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?dps_pid=IE13097737&manifest=true => IIIF format


            icandid_input  = IcandidCollector::Input.new()
            output = DataCollector::Output.new
            begin

                url = d['_resourceIdentifier'].gsub("representation","metadata")

                url = "https://repository.teneo.libis.be/delivery/DeliveryManagerServlet?dps_pid=#{d['$text']}&manifest=true"

#                url = "file:///source_records/rosetta/manifest-#{d['$text']}.json"

                o[:id] = d['$text']

                data = icandid_input.collect_data_from_uri(url:  url  ,  options: o )
                rules_ng.run( ROSETTA_RULES[:rs_records], data, output, o )
                rdata = output.data[:records]

                # pp "-----------------------------------------"
                # pp rdata
                # pp "-----------------------------------------"
                
                # source_records_dir = o[:config][:query][:enrichment][:source_records_dir]

                #output.data[:records][:hasPart].map{ |p| p[:@id] }.flatten.each { |rosetta_file_id|
                #    pp rosetta_file_id.split("_").last
                #    Dir["#{source_records_dir}/#{rosetta_file_id}*.json"].each do |enrichment_file| 
                #        pp enrichment_file
                #    end
                #}

#                pp "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
#                pp o[:config][:query][:enrichment][:source_records_dir]
#                pp "ooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
             
                

                # pp "-----------------------------------------"

                # exit


                # pp rdata[:isBasedOn]
                
                rdata.delete(:isBasedOn)
            rescue StandardError => e
                pp e
                rdata = nil
            end
            
            rdata
        } }
    } 
}
