#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

DEBUG = false

RULE_SET_v1_1 = {
    version: "1.1",
    rs_next_value: {
        page: { "$" => lambda { |d,o| 
            o["page"]+1
        }},
        total: "$.count" ,
        lastindex: "$.lastindex"
    },
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
            unless (d["result"].empty? || d["count"] == 0)
                first_id = d["result"].first["id"].split('/').last
                last_id = d["result"].last["id"].split('/').last
                "#{first_id}_#{last_id}.json"
            end
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
        records: { "$" => [ lambda { |d,o|
        
            out = DataCollector::Output.new

            unless d["data"].nil?
              rules_ng.run(RULE_SET_v1_1[:rs_record], d["data"]["result"], out, o)
            else
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #
                # In de source directory zitten niet enkel json-bestanden van de search results.
                # voorbeeld: https://ws.vlpar.be/e/opendata/jln/1681886 en https://ws.vlpar.be/e/opendata/debat/1681738
                # Deze worden niet verwerkt door de parser.
                # pp d.keys()
                # pp d["id"]
                # pp d["titel"]
                # !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #
            end
            
            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$" => lambda { |d,o| 

            rdata = {}
            reorgenizeddata = {}
            d["metatags"]["metatag"].each { |metatag|
                unless reorgenizeddata.has_key?(metatag["name"])
                    reorgenizeddata[metatag["name"]] = metatag["value"]
                else
                    unless reorgenizeddata[metatag["name"]].is_a?(Array)
                        reorgenizeddata[metatag["name"]] = [ reorgenizeddata[metatag["name"]] ]
                    end
                    reorgenizeddata[metatag["name"]] << metatag["value"]
                end
            }
            # reorgenizeddata = reorgenizeddata.raw

            if reorgenizeddata["document"].nil?
                @logger.warn(" document is missing in record id : #{ d["id"] } ")
                @logger.warn(" document is missing in displayurl : #{ reorgenizeddata["displayurl"] } ")
            end

            #unless reorgenizeddata["document"].nil?
            #    if reorgenizeddata["document"].match(/docs.vlaamsparlement.be\/pfile/)
            #        if reorgenizeddata["documenttype"] != "tekst"
            #            pp "record id : #{ d["id"] } "
            #            pp "record url : #{ d["url"] } "
            #            pp " documenttype:  #{ reorgenizeddata["documenttype"]  }: #{reorgenizeddata["document"] }"
            #        end
            #    else
            #        pp "record id : #{ d["id"] } "
            #        pp "record id : #{ d["url"] } "                    
            #        pp "  documenttype [Geen pfile]: #{ reorgenizeddata["documenttype"]  }: #{reorgenizeddata["document"] } !!!!!!!!!!!!!!!!!!"
            #    end
            #end
                                   
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_id], d, out, o)
            o[:id] = out[:id].first

            if reorgenizeddata["documenttype"].nil?
                @logger.warn(" documenttype is missing in record id : #{ d["id"] }")
                @logger.warn(" documenttype is missing in displayurl : #{ reorgenizeddata["displayurl"] } ")
                return nil 
            end
            
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_1[:rs_document], reorgenizeddata, out, o)
            o[:documenttype] = out[:documenttype].first()
            o[:document]  = out[:document]
            o[:record_id] = d["id"]
            out.clear

            unless ["tekst", "bijlage", "handeling", "vraag aan minister", "antwoord", "vraag en antwoord"].include? ( o[:documenttype].downcase )
                pp "-------------------------------------------------------------------------------------"
                pp "===>   New documenttype [#{ o[:documenttype].downcase }] detected"
                pp "-------------------------------------------------------------------------------------"
                pp ""
                pp ""
                
                exit
            end

            rdata[:sameAs] = d["url"]

            rules_ng.run(RULE_SET_v1_1[:rs_record_data], reorgenizeddata, out, o)

            rules_ng.run(RULE_SET_v1_1[:rs_contacttype], reorgenizeddata["opendata"], out, o)
            rdata.merge!(out.data)
            out.clear
            rdata.compact
            begin
                # During downloading, the opendata link is replaced by the http response.
                # If something went wrong, the string "Error downloading ...." is invested in the opendata property
                if reorgenizeddata["opendata"].is_a?(String) && reorgenizeddata["opendata"].match(/^Error downloading/)
                    rdata = nil
                    @logger.warn ("reorgenizeddata[\"opendata\"] was not replace with data : #{ o[:record_id]  }")
                    return nil
                    raise "reorgenizeddata[\"opendata\"] was not replace with data : #{ o[:record_id]  }"
                end
                if rdata[:associatedMedia].nil? && o[:document].nil?
                    pp "Geen rdata[:associatedMedia].nil? || rdata[:sameAs].nil? in  : #{ o[:record_id]  }"
                    link_self = reorgenizeddata["opendata"]["link"]&.select { |item| item["rel"] == "self" }&.first
                    link_verslag = reorgenizeddata["opendata"]["link"]&.select { |item| item["rel"] == "verslag" }&.first
                    unless link_verslag.nil? || link_self.nil?
                        if link_verslag["href"] == "#{link_self["href"]}/verslag"
                            rdata[:associatedMedia] = {
                                :@type         => "MediaObject",
                                :contentUrl    => link_verslag["href"],
                                :name          => "Download (JSON)",
                                :encodingFormat => "application/json",
                            }
                        else
                            raise "link_verslag[\"href\"] == \"\#{link_self[\"href\"]}/verslag\"\n"
                        end
                    else
                        raise "rdata[:associatedMedia].nil? && o[:document].nil? in : #{ o[:record_id]  }"
                    end
                end
            rescue Exception => e
                pp "-------- reorgenizeddata --------------------------------------------------------"
                pp reorgenizeddata
                pp "-------- rdata ------------------------------------------------------------------"
                pp rdata
                pp "---------------------------------------------------------------------------------"
                @logger.error ("Issue in data: #{e.message}")
                exit()
            end

            unless o[:document].nil?
                o[:document] = o[:document].sort.uniq

                if o[:document].size > 1
                    pp "More than 1 entry in o[:document]  record id : #{ o[:record_id]  }"
                    pp  o[:document]
                    exit
                end

                pdf_id = o[:document].first.split('=').last

                file_to_check = File.join( File.dirname(o["file"]), "#{pdf_id}.pdf")
                begin
                    pdf_data = File.open(file_to_check)
                rescue Errno::ENOENT
                    pp "#{file_to_check} Not found! record id : #{ o[:record_id]  }"
                    pp "try to fetch it from the internet #{ o[:document].first }?"
                    input_options = {
                        number_of_retries: 5,
                        headers: {"Content-Type" => "application/pdf", "accept-encoding" => "UTF-8", "Accept" => "application/pdf"}
                    }
                    file = @icandid_input.download_file_from_uri(url: o[:document].first,  download_path: file_to_check, options: input_options )
                    if file[:content_type] != "application/pdf"
                        @logger.warn ("Downloaded file from #{ o[:document].first } is of type [ #{ file[:content_type] } ]")
                        @logger.warn ("File is saved as #{file_to_check}")
                        @logger.warn ("Full text extraction for this document format available ????")
                    end
                    pdf_data = File.open( file_to_check )
                end
                unless pdf_data.nil?
                    rdata[:text] = @icandid_utils.tikaFullTextExtraction( pdf_data )
                    # pp  rdata[:text].size
                    # rdata[:text] = "Get full text from #{file_to_check} with Tika"
                end
            end
            # TODO : record maken voor de beschrijving van een planiare vergadering.
            # Deze zitten niet in de download en het is geen goed idee om in elk onderdeel
            # de full text extractie van de volledige vergadering te steken.
            # een debat https://www.vlaamsparlement.be/nl/parlementaire-documenten/debatten/1438190 
            # in een onderdeel van een vergadering en hiervan is er een json beschijving https://ws.vlpar.be/e/opendata/jln/1439830
            # Van de volledige vergadering (https://www.vlaamsparlement.be/nl/parlementair-werk/plenaire-vergaderingen/1434121)
            #  is er een pdf (https://docs.vlaamsparlement.be/pfile?id=1613348) en vaak ook een You tube 

            # TODO: niet alle https://docs.vlaamsparlement.be/pfile?id= -request refereren naar een pdf-document
            # Bij het afhalen van deze documenten werd daar wel vanuit gegaan :-(
            # Daarnaast is het ook de vraag of tike daarmee overseg kan
            # voorbeeld: http://be.vlp.feed/900718/pfls/490915 (https://www.vlaamsparlement.be/parlementaire-documenten/schriftelijke-vragen/900718) 
            # met document https://docs.vlaamsparlement.be/pfile?id=490915 (vrg.088.doc en antw.088.docx)
            
            if rdata[:text].nil?
                unless rdata[:associatedMedia].nil?

                    rdata[:associatedMedia] = [ rdata[:associatedMedia] ] unless rdata[:associatedMedia].is_a?(Array)
                    rdata[:associatedMedia] = rdata[:associatedMedia].uniq

                    #pp "TEST rdata[:associatedMedia]rdata[:associatedMedia]rdata[:associatedMedia]"
                    #pp rdata[:associatedMedia]

                    rdata[:associatedMedia].map { |a| 
                        if a[:encodingFormat] == "application/json"
                            
                            input_options = {
                                number_of_retries: 3,
                                headers: {"Content-Type" => "application/json", "accept-encoding" => "UTF-8", "Accept" => "application/json"}
                            }
                            
                            data =  @icandid_input.collect_data_from_uri(url: a[:contentUrl], options: input_options )
                            
                            a[:name] = "#{ data["titel"] } [ Download (JSON) ]"
                            # a[:caption] = JSON.generate( data )
                            a[:text] = JSON.generate( data )
                        end
                        a
                    } 
                end
            end

            if rdata[:text].nil? and [rdata[:associatedMedia]]&.flatten.select { |a| ! a[:text].nil? }.empty?
                pp "Geen rdata[:text] in  : #{ o[:record_id]  }"
                exit()
            end
            rdata
            
        } }
    },
    rs_id:{
        id:  {'$.id' =>  lambda { |d,o|
            #unless d.match(/pfls/)
            #    pp "==========================================================>>>>> #{d}"
            #else
            #    pp "!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!! #{d}"
            #end
            "#{o[:ingest_data][:dataset][:@id].downcase}_#{d.split('/').last}-00000"
        }}
    },
    rs_document:{
        #################################################################################################
        # _soort: "$.soort", # ==> PI,
        #################################################################################################
        document:  [ 
            {
                '$.document' => lambda { |d,o| 
                    if d.match(/^https:\/\/docs.vlaamsparlement.be\/pfile\?id=/)
                        d
                    end
            }},
            { 
                "$.?????????????opendata..vergadering..pdffilewebpath"=> lambda { |d,o| 
                if d.match(/^https:\/\/docs.vlaamsparlement.be\/pfile\?id=/)
                    d
                end
            }} 
        ],
        documenttype: '$.documenttype'
    },
    rs_record_data: {      
        name:     {'$' => lambda { |d,o| 
        # Elk record heeft een initiatief en een titel
        # Als het om een echt PDF-document gaat is er ook een documenttitel
        # Als er een documenttitel is wordt dit de name-property en wordt de titel de headline-property

            if d["initiatief"].nil?
                pp "MISSING initiatief" 
                exit
            end
            if d["documenttitel"].nil?
                "#{d["initiatief"]}: #{d["titel"]}"
            else
                "#{d["initiatief"]}: #{d["documenttitel"]}"
            end
        }},
        headline:        {'$' => lambda { |d,o| 
            unless d["documenttitel"].nil?
                d["titel"]
            end
        }},

        keywords:        '$.thema',
        datePublished:   '$.publicatiedatum',
        legislationType: ['$.aggregaat','$.aggregaattype'],
        url:             '$.displayurl',
        description:     '$.onderwerp',
        #description:     {'$' => lambda { |d,o| 
        #    out = DataCollector::Output.new
        #    rules_ng.run(RULE_SET_v1_1[:rs_description], d, out, o)
        #    if out.data[:description].nil?
        #        out.data[:onderwerp]
        #    else
        #        pp "OUTPUT DESCRIPTION: #{out.data[:description][0][0..50]}"
        #        out.data[:description]
        #    end
        #}},
        ################################################################################################# 
        #identifier:       {'$' => lambda { |d,o| 
        #    {
        #        :@type => "PropertyValue",
        #        :@id   => "documentnumber",
        #        :name  => "Documentnummer",
        #        :value => "#{d["nummer"]} (#{d["zittingsjaar"]})#{ " Nr. #{d["volgnummer"]}" unless d["volgnummer"].nil? }"
        #    }
        #}},
        publisher:        {'$' => lambda { |d,o| 
            {
                :@type => "Organization",
                :@id   => "iCANDID_ORGANIZATION_VLAAMS_PARLEMENT",
                :name  => "Vlaams Parlement"
            }
        }},
        #################################################################################################        
        #_mimetype: '$.mimetype',

        #################################################################################################
        # Kan basisstuk worden gelinkt via isPartOf aan PI (parlementair initiatief) 
        # isPartOf: 
        #   {
        #      @type:
        #      "name": {@value= <basisstuk>,"@language" : "nl-Latn"},
        #      "@id" : "iCANDID_vlaamsparlement_<??????>_1700751533909131312"
        #   }
        # 
        #  _basisstuk: '$.basisstuk',
        #################################################################################################

        # records met documenttype=handeling
        # bezitten in document een link naar de webversie van het verslag van de plenaire vergadering
        # de link naar de pdf versie van dat verslag zit in opendata.journaallijn..vergadering.plenairehandelingen.pdffilewebpath
        # regex match als extra controle dat het naar een pfile gaat !!
        # Wordt het verslag van de plenaire vergadering niet afzonderlijk als record beschreven en afgehaald ??? 
        # sameAs: [ 
        #     {
        #         '$.document' => lambda { |d,o| 
        #             if d.match(/^https:\/\/docs.vlaamsparlement.be\/pfile\?id=/)
        #                 d
        #             end
        #     }}
        #    ,
        #    { 
        #        "$.opendata..vergadering..pdffilewebpath"=> lambda { |d,o| 
        #        if d.match(/^https:\/\/docs.vlaamsparlement.be\/pfile\?id=/)
        #            d
        #        end
        #    }}
        #],

        associatedMedia:  [
            {
                "$.document" => lambda { |d,o|
                    if d.match(/^https:\/\/docs.vlaamsparlement.be\/pfile\?id=/)
                        {
                            :@type         => "MediaObject",
                            :contentUrl    => d,
                            :name          => "Download (PDF)",
                            :encodingFormat => "application/pdf"
                        }
                    end
                }
            },
            {
                "$.opendata.bijlage" => lambda { |d,o|
                    unless d["url"].nil?
                        if o[:documenttype] == "bijlage"
                            if d["url"].match(/^https:\/\/docs.vlaamsparlement.be\/files\/pfile\?id=/)
                                {
                                    :@type         => "MediaObject",
                                    :contentUrl    => d["url"],
                                    :name          => "#{ d["titel"] } [ Download (PDF) ]",
                                    :encodingFormat => "application/pdf"
                                }
                            else
                                @logger.error ("bijlage die geen pdf is [#{d["url"]}] !!!  record id : #{o[:id]}) ")
                                exit
                            end
                        end
                    end
                }
            },
            {
                "$.opendata.procedureverloop..vergadering.plenairehandelingen.pdffilewebpath" => lambda { |d,o|
                    if o[:documenttype] == "handeling"
                        if d.match(/^https:\/\/docs.vlaamsparlement.be\/files\/pfile\?id=/)
                            {
                                :@type         => "MediaObject",
                                :contentUrl    => d,
                                :name          => "Plenaire vergadering [ Download (PDF) ]",
                                :encodingFormat => "application/pdf"
                            }
                        else
                            @logger.error ("handeling die geen pdf is [#{d["url"]}]  !!!  record id : #{o[:id]}) ")
                            exit
                        end
                    end
                }
            },
            {
                "$.opendata.procedureverloop..journaallijn..link[?(@.rel == 'self')].href" => lambda { |d,o|
                    # pp "-------------- associatedMedia ----------------- $.opendata..link[?(@.rel == 'self')].href --------------------------------"
                    # pp d
                    # pp "#{d.split('/').last().split('?').first()} == #{o[:record_id].split('/').last()}"
                    if d.split('/').last().split('?').first() == o[:record_id].split('/').last()
                        {
                            :@type         => "MediaObject",
                            :contentUrl    => d,
                            :name          => "Download (JSON)",
                            :encodingFormat => "application/json",
                            #:name          => "#{ data["titel"] } [ Download (JSON) ]",
                            #:caption       => JSON.generate( data )
                        }
                    end
                }
            },
            {
                "$.opendata.journaallijn..vergadering.video-youtube-id" => lambda { |d,o|
                    {
                        :@type         => "VideoObject",
                        :contentUrl    => "https://www.youtube.com/watch?v=#{d}",
                        :name          => "Herbekijk de opname"
                    }
                }
            }
        ]
    },
    rs_description: {
        description: '$.opendata.journaallijn.vergadering.omschrijving',
        onderwerp: '$.onderwerp'
    },
    rs_contacttype: {
        legislationPassedBy: { "$.contacttype[?(@.beschrijving == 'Indiener')]" =>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        legislationResponsible: { "$.contacttype[?(@.beschrijving == 'Bevoegde minister')]" =>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        author: { "$.contacttype[?(@.beschrijving == 'Verslaggever')]" =>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }}
    },
    rs_contact: {
        contact: { "$" =>  lambda { |d,o|
            if d["id"]
                rdata = {
                    :@type => "Person",
                    :@id   => "#{o[:prefixid]}_PERSON_id_#{ d["id"] }",
                    :name  => "#{d["voornaam"]} #{d["naam"]}"
                }
            else
                rdata = {
                    :@type => "Organization",
                    :@id   => "#{o[:prefixid]}_ORGANIZATION_#{ o[:index] }",
                    :name  => d["naam"]
                }
            end
            if d["fractie"]
                rdata["memberOf"] = {
                    :@type => "Organization",
                    :@id   => "#{o[:prefixid]}_ORGANIZATION_id_#{d["fractie"]["id"] }",
                    :name  => d["fractie"]["naam"]
                }
            end
            rdata
        }}
    }
}