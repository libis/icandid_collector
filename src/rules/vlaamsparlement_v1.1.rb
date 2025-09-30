#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
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

            if reorgenizeddata["opendata"].is_a?(String)
                @logger.warn("No opendata in record id : #{ d["id"] } ")
                @logger.warn("No opendata document : #{ reorgenizeddata["document"] } ")
                @logger.warn("No opendata displayurl : #{ reorgenizeddata["displayurl"] } ")
                begin
                    data =  @icandid_input.collect_data_from_uri(url:  reorgenizeddata["displayurl"] , options: {} )
                rescue  Exception => e
                    if  /^Unable to process received status code = 503/ =~ e.message
                        @logger.warn("Unable to process received status code = 503")
                        return nil
                    end
                end
                @logger.warn("Try to do something with this data  ")
            end

            if reorgenizeddata["document"].nil?
                @logger.warn(" document is missing in record id : #{ d["id"] } ")
                @logger.warn(" document is missing in displayurl : #{ reorgenizeddata["displayurl"] } ")
               
                #pp "-------- rdata ------------------------------------------------------------------"
                #pp rdata
                #pp "---------------------------------------------------------------------------------"
            end
                                   
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_id], d, out, o)
            o[:id] = "#{o[:ingest_data][:dataset][:@id].downcase}_#{ out[:id].first }-00000"
            o[:prefixid] = "#{o[:ingest_data][:dataset][:@id].downcase}_#{ out[:id].first }"
            o[:record_id] = out[:id].first 
            o[:source_id] = d["id"]
            o[:index] = 0
            
            

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            document_out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_document], reorgenizeddata, document_out, o)
            
            unless document_out[:document].nil?
                document = document_out[:document].sort.uniq

                if document.size > 1
                    pp "More than 1 entry in document  record id : #{ o[:record_id]  }"
                    pp  document
                    exit
                end
                document = document.first
                o[:document] = document
            end
            o[:documenttype] = document_out[:documenttype]&.first

            opendata_out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_opendata], reorgenizeddata["opendata"], opendata_out, o)
           
            if ( ! opendata_out[:bestand_ordered].nil? && opendata_out[:bestand_ordered].is_a?(Array) && opendata_out[:bestand_ordered].size > 2 )
                    @logger.warn "bestand_ordered.is_a?(Array) #{opendata_out[:bestand_ordered].is_a?(Array)}"
                    @logger.warn "bestand_ordered.size #{opendata_out[:bestand_ordered].size }"
                    @logger.warn "bestand_ordered #{opendata_out[:bestand_ordered]}"
                    @logger.warn "bestand_ordered #{  o[:source_id] }"
                    @logger.warn "bestand_ordered #{ reorgenizeddata["displayurl"] }"
                    # pp opendata_out[:bestand_ordered]
                    # pp reorgenizeddata["displayurl"]
                    # pp reorgenizeddata["document"]
            end


            opendata_link_verslag = opendata_out[:link_verslag]&.first 
            opendata_link_verslag = nil if opendata_link_verslag&.include?("Error") 

            opendata_link_self = opendata_out[:link_self]&.first
            opendata_link_self = nil if opendata_link_self&.include?("Error") 

            rdata[:sameAs] = d["url"]

            rules_ng.run(RULE_SET_v1_1[:rs_record_data], reorgenizeddata, out, o)

            rules_ng.run(RULE_SET_v1_1[:rs_contacttype], reorgenizeddata["opendata"], out, o)
            rdata.merge!(out.data)
            out.clear
            rdata.compact

            # Get Full text !
            # 1)
            # Als de URI in de property document 'pfile' bezit wordt deze pdf  (of toch meestal pdf) opgehaald
            # de full text wordt via tika geextrgeerd en toegevoegd aan het record in de property text
            # 2) 
            # Als er geen document is maar er is wel een document in de opendata (en deze bezit 'pfile')
            # en de id in de opendata is te linken aan de id op het hoofdniveau,  wordt deze pdf  (of toch meestal pdf) 
            # opgehaald de full text wordt via tika geextrgeerd en toegevoegd aan het record in de property text
            # !!!!!!!!!!!!!!!!!! Hier moet het initiateif wel een veralg zijn ?????????????????
            # 3)
            # '$.opendata.link[?(@.rel == \'self\')].href', en '$.opendata.link[?(@.rel == \'verslag\')].href' bestaan en verwijzen naar dezelfde id
            # de json ophalen en de waarde uit verslag-tekst toegevoegd aan het record in de property text
            # 4)
            # records zonder document of document verwijst niet naar een pfile en er is een opendata.journaallijn..link[?(@.rel == 'self')].href
            # zal de json-response van de url worden opgenomen in associatedMedia
            # 5)
            # Als aan geen van bovenstaande voorwaarden is voldaan, wordt het record niet verwerkt (retrun nil) als de properties
            #    (
            #       ["VI","PI"].include?( document_out[:soort]&.first ) &&
            #        ["Vraag om uitleg","Voorstel van decreet","Voorstel van resolutie"].include?(document_out[:initiatief]&.first) &&
            #        ["ingetrokken","omgevormd tot actualiteitsdebat"].include?( document_out[:status]&.first ) &&
            #        o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)
            #     
            #    ) ||
            #    (
            #        ["VZS"].include?( document_out[:soort]&.first ) &&
            #        ["Verzoekschrift"].include?(document_out[:initiatief]&.first) &&
            #        ["afgehandeld in plenaire vergadering","verwezen naar commissie"].include?( document_out[:status]&.first )
            #    )
            # 
            # http://be.vlp.feed/1854886
            # https://www.vlaamsparlement.be/nl/parlementaire-documenten/gedachtewisselingen-hoorzittingen/1854886 "Verslag in voorbereiding"
            # https://www.vlaamsparlement.be/nl/parlementaire-documenten/parlementaire-initiatieven/1863968 "ingediend"
            #
            # https://www.vlaamsparlement.be/parlementaire-documenten/gedachtewisselingen-hoorzittingen/1853349
            # https://www.vlaamsparlement.be/nl/parlementaire-documenten/verzoekschriften/1829642
            # https://www.vlaamsparlement.be/parlementaire-documenten/gedachtewisselingen-hoorzittingen/1829661
            #

            unless document.nil?
                if document.match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/)
                    document_to_check = document
                end
            else                            
                # opendata.id == 1829073
                # id == /http:\/\/be.vlp.feed\/1829073$/
                # https://docs.vlaamsparlement.be/pfile?id=2066332

                opendata_id = opendata_out[:id]&.first
                opendata_document = opendata_out[:document]&.first
                unless opendata_document.nil?
                    unless opendata_link_verslag.nil? || opendata_link_self.nil?
                        @logger.warn ("opendata_out has also link_verslag and link_self. Get dat from data[\"verslag-tekst\"] ? ")
                        pp "Not yet implemented"
                        pp "-------------------- record d---- -----------------------------------------------------"
                        exit
                    end
                    if (
                        d["id"] == "http://be.vlp.feed/#{opendata_id}" && 
                        opendata_document.match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/) 
                        # &&
                        #[
                        #    "Verslag",
                        #    "Verslag van de hoorzitting",
                        #    "Voorstel van resolutie",
                        #    "Tekst aangenomen door de plenaire vergadering"
                        #    "Ontwerp van decreet"
                        #].include?(document_out[:initiatief]&.first) 
                    )
                        @logger.warn ("Document is nil but opendata.document has link to pfile #{ opendata_document}")
                        document_to_check = opendata_document
                    end
                else
                    # Geen opendata_document
                    # retry downloading opendata

                    opendata_url = "https://ws.vlpar.be/e/opendata/#{ document_out[:soort]&.first.downcase }/#{ o[:record_id] }"

                    @logger.debug ("retry downloading opendata: #{opendata_url}")

                    input_options = {
                        number_of_retries: 3,
                        headers: {"Content-Type" => "application/json", "accept-encoding" => "UTF-8", "Accept" => "application/json"}
                    }
                    opendata_data =  @icandid_input.collect_data_from_uri(url: opendata_url , options: input_options )
                    
                    #pp "opendata_data"
                    #pp opendata_data["filewebpath"]
                    #pp opendata_data["status"]
                    #pp opendata_data.keys

                    unless opendata_data.nil? || opendata_data =="no data found"
                        unless opendata_data["filewebpath"].nil?
                            document_to_check = opendata_data["filewebpath"]
                        end
                    end
                end
            end

            unless document_to_check.nil?
                begin
                    pdf_id = document_to_check.split('=').last
                    file_to_check = File.join( File.dirname(o["file"]), "#{pdf_id}.pdf")
                    pdf_data = File.open(file_to_check)
                rescue Errno::ENOENT
                    # pp "#{file_to_check} Not found! record id : #{ o[:record_id]  }"
                    # pp "try to fetch it from the internet #{ document }?"
                    input_options = {
                        number_of_retries: 5,
                        headers: {"Content-Type" => "application/pdf", "accept-encoding" => "UTF-8", "Accept" => "application/pdf"}
                    }
                    file = @icandid_input.download_file_from_uri(url: document_to_check,  download_path: file_to_check, options: input_options )
                    if file[:content_type] != "application/pdf"
                        @logger.warn ("Downloaded file from #{ document_to_check } is of type [ #{ file[:content_type] } ]")
                        @logger.warn ("File is saved as #{file_to_check}")
                        @logger.warn ("Full text extraction for this document format available ????")
                    end
                    pdf_data = File.open( file_to_check )
                end
                unless pdf_data.nil?
                    dir = File.dirname(o[:file])
                    pdf_id = File.basename(file_to_check, ".pdf")
                    rdata[:text] = @icandid_utils.tikaFullTextExtraction( pdf_data, {file:  File.join( dir, "#{pdf_id}.txt") } )
                end
            end
    
            if rdata[:text].nil?
                unless opendata_link_verslag.nil? || opendata_link_self.nil?
                    
                    ids = o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)\/pfls\/(\d*)/)
                    id = opendata_link_self.split("/").last
   
                    if ids[1] == id
                        if "#{opendata_link_self}/verslag" == "#{opendata_link_verslag}"
                            @logger.info "Get full text from #{ opendata_link_verslag}"

                            input_options = {
                                number_of_retries: 3,
                                headers: {"Content-Type" => "application/json", "accept-encoding" => "UTF-8", "Accept" => "application/json"}
                            }
                            data =  @icandid_input.collect_data_from_uri(url: opendata_link_verslag , options: input_options )
                            rdata[:text] = data["verslag-tekst"]
                        end
                    else
                        pp "opendata_link_self #{opendata_link_self}"
                        pp "opendata_link_verslag #{opendata_link_verslag}"
                        exit
                    end
                end
            end

            if rdata[:text].nil? && rdata[:associatedMedia].nil?
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] in : #{ o[:record_id]} / #{o[:source_id]} file #{o[:file]}"
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] in displayurl : #{ reorgenizeddata["displayurl"]  }"
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] in openurl https://ws.vlpar.be/e/opendata/#{ document_out[:soort]&.first.downcase }/#{ o[:record_id] }"
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] document_out soort: #{ document_out[:soort]}, initiatief : #{  document_out[:initiatief] }, status: #{  document_out[:status] }"
                

                if (
                (
                    ["VI"].include?( document_out[:soort]&.first ) &&
                    ["Vraag om uitleg"].include?(document_out[:initiatief]&.first) &&
                    ["ingetrokken","omgevormd tot actualiteitsdebat","verwezen naar commissie"].include?( document_out[:status]&.first ) &&
                    o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)
                    
                ) || (
                    ["PI"].include?( document_out[:soort]&.first ) &&
                    ["Voorstel van decreet","Voorstel van resolutie"].include?(document_out[:initiatief]&.first) &&
                    ["ingetrokken","verwezen naar commissie"].include?( document_out[:status]&.first ) &&
                    o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)

                ) || (
                    ["VZS"].include?( document_out[:soort]&.first ) &&
                    ["Verzoekschrift","Gedachtewisseling"].include?(document_out[:initiatief]&.first) &&
                    [
                        "afgehandeld in plenaire vergadering",
                        "afgehandeld in commissie","verwezen naar commissie",
                        "in behandeling in commissie",
                        "kennisneming door de commissie",
                        "ingediend"
                    ].include?( document_out[:status]&.first ) &&
                    o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)
                ) || (
                    ["HG"].include?( document_out[:soort]&.first ) &&
                    ["Verslagmoment van de Europese Ministerraden","Gedachtewisseling","Hoorzitting"].include?(document_out[:initiatief]&.first) &&
                    ["Verslag in voorbereiding","te behandelen in commissie","in behandeling in commissie","beslissing commissie"].include?( document_out[:status]&.first ) &&
                    o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)
                ) || (
                    ["PI"].include?( document_out[:soort]&.first ) &&
                    ["Verslag","Conceptnota voor nieuwe regelgeving"].include?(document_out[:initiatief]&.first) &&
                    ["ingediend","verwezen naar commissie"].include?( document_out[:status]&.first ) &&
                    o[:source_id].match(/http:\/\/be.vlp.feed\/(\d*)$/)
                )
                )
                    return nil
                end

                # "http://be.vlp.feed/1662121" en "http://be.vlp.feed/1662121/pfls/2065246" verwijzen naar https://www.vlaamsparlement.be/parlementaire-documenten/parlementaire-initiatieven/1662121
                # maar enkel http://be.vlp.feed/1662121/pfls/2065246 heeft document = "https://docs.vlaamsparlement.be/pfile?id=2065246"

                @logger.error "Geen rdata[:text] of rdata[:associatedMedia][:text] document_out soort: #{ document_out[:soort]}, initiatief : #{  document_out[:initiatief] }, status: #{  document_out[:status] }"
                return nil
            end

            if rdata[:text].nil? and [rdata[:associatedMedia]]&.flatten.select { |a| ! a[:text]&.nil? }.empty?
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] in : #{ o[:record_id]} / #{o[:source_id]}"
                #pp "-------  rdata ------------------------------- "
                #pp rdata
                #pp "-------  reorgenizeddata[\"displayurl\"] ------------------------------- "
                #pp reorgenizeddata["displayurl"]
                #pp "-------  reorgenizeddata[\"url\"] ------------------------------- "
                #pp reorgenizeddata["url"]
                #pp "-------  reorgenizeddata[\"document\"] ------------------------------- "
                #pp reorgenizeddata["document"]
                #pp "-------  reorgenizeddata[\"opendata\"] ------------------------------- "
                #pp reorgenizeddata["opendata"]
                #pp "Geen rdata[:text] of rdata[:associatedMedia][:text] in : #{ o[:record_id]  }"
                @logger.warn "Geen rdata[:text] of rdata[:associatedMedia][:text] in : #{ o[:record_id]} / #{o[:source_id]} file #{o[:file]}"
                return nil
                #raise "Geen rdata[:text] of rdata[:associatedMedia][:text] in : #{ o[:record_id]} / #{o[:source_id]} file #{o[:file]}"
            end
            rdata
            
        } }
    },
    rs_id:{
        id:  {'$.id' =>  lambda { |d,o|
            regex = /^http:\/\/be.vlp.feed\/([0-9]*)\/pfls\/([0-9]*)/
            if match = d.match(regex)
                id = "#{match[1]}_#{match[2]}"
            end
            regex = /^http:\/\/be.vlp.feed\/([0-9]*)$/
            if  match = d.match(regex)
                id = match[1]
            end            
            regex = /([0-9]*)\/jnl\/([0-9]*)/
            if  match = d.match(regex)
                id = "#{match[1]}_#{match[2]}"
            end

            if id.nil?
                @logger.error ("No id found !!!!!!!!!!!!!")
                pp o
                raise "No id found !!!!!!!!!!!!!"
                exit
            end
            return id
        }}
    },
    rs_opendata:{
        id: '$.id',
        document: '$.document.url',
        bestand_ordered: '$.bestand-ordered',
        filewebpath: '$.filewebpath',
        link_self: '$.link[?(@.rel == \'self\')].href',
        link_verslag: '$.link[?(@.rel == \'verslag\')].href',
        journaallijn:{ "$.journaallijn..link[?(@.rel == 'self')].href" => lambda { |d,o|
            if d.match(/^https:\/\/ws.vlpar.be\/e\/opendata\/jln\//)
                d
            end
        }}
    },
    rs_document:{
        document: '$.document',
        status:'$.status',
        soort:'$.soort',
        initiatief:'$.initiatief',
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
        legislationJurisdiction: '$.bevoegdheid',
        url:             '$.displayurl',
        description:     '$.onderwerp',
        creativeWorkStatus: '$.status',
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
        identifier:       {'$' => lambda { |d,o| 
            {
                :@type => "PropertyValue",
                :@id   => "documentnumber",
                :name  => "Documentnummer",
                :value => "#{d["nummer"]} (#{d["zittingsjaar"]})#{ " Nr. #{d["volgnummer"]}" unless d["volgnummer"].nil? }"
            }
        }},
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
                "$" => lambda { |d,o|
                    if d["document"]&.match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/)
                        if d["mimetype"] != "application/pdf"
                            @logger.warn "Strange mimetype : #{ d["mimetype"] } / #{ d["document"] }"
                        end
                        {
                            :@type         => "MediaObject",
                            :contentUrl    => d["document"],
                            :name          => "Download (PDF)",
                            :encodingFormat => d["mimetype"],
                        }
                    end
                }
            },
            {
                "$.opendata.bijlage" => lambda { |d,o|
                    unless d["url"].nil?
                        if d["doel"] == "BIJLAGE"
                            if d["url"].match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/) 
                                if d["url"].split('id=').last != o[:record_id]
                                    {
                                        :@type         => "MediaObject",
                                        :contentUrl    => d["url"],
                                        :name          => "#{ d["titel"] } [ Download (PDF) ]",
                                        :encodingFormat => "application/pdf"
                                    }
                                end
                            else
                                @logger.error ("bijlage die geen pdf is [#{d["url"]}] !!!  record id : #{o[:id]}) ")
                                raise "bijlage die geen pdf is [#{d["url"]}] !!!  record id : #{o[:id]}) "
                                exit
                            end
                        end
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
            },

            # Als het document een bijlage is bevat opendata het document waartoe deze bijlage behoord
            {
                "$.opendata[?(@.filewebpath)]" => lambda { |d,o|
                    if o[:documenttype] == "bijlage"
                        if d["filewebpath"]&.match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/)
                            rdata = {
                                :@type         => "MediaObject",
                                :contentUrl    => d["filewebpath"],
                                :name          => "#{ d["titel"] } [ Download (PDF) ]",
                                :encodingFormat => "application/pdf"
                            }
                        end
                    end
                }
            },

            {
                "$.opendata[?(@.bestand-ordered)]" => lambda { |d,o|
                    if d["bestand-ordered"]&.match(/^https:\/\/docs.vlaamsparlement.be(\/files)?\/pfile\?id=/)
                        rdata = {
                            :@type         => "MediaObject",
                            :contentUrl    => d["bestand-ordered"],
                            :name          => "#{ d["titel"] } [ Download (PDF) ]",
                            :encodingFormat => "application/pdf"
                        }
                    end
                }
            },

            {
                "$.opendata.journaallijn..link[?(@.rel == 'self')].href" => lambda { |d,o|
                    journaallijn = d
                    if journaallijn.match(/^https:\/\/ws.vlpar.be\/e\/opendata\/jln\//)
                        if o[:document].match(/^https:\/\/www.vlaamsparlement.be\/.*\/verslag/)
                            input_options = {
                                number_of_retries: 3,
                                headers: {"Content-Type" => "application/json", "accept-encoding" => "UTF-8", "Accept" => "application/json"}
                            } 
                            data =  @icandid_input.collect_data_from_uri(url: journaallijn, options: input_options )

                            if data.nil?
                                @logger.warn "No data returned for journaallijn #{journaallijn}: #{ o[:record_id]} / #{o[:source_id]}"
                            else
                                rdata = {
                                    :@type          => "MediaObject",
                                    :contentUrl     => journaallijn,
                                    :encodingFormat => "application/json",
                                    :name           => "#{ data["titel"] } [ Download (JSON) ]",
                                    :text           => JSON.generate( data )
                                }
                            end  
                        end
                    end
                }
            }

        ]
    },
    rs_description: {
        description: '$.opendata.journaallijn.vergadering.omschrijving',
        onderwerp: '$.onderwerp'
    },
    rs_contacttype: {
        legislationPassedBy: [
            { "$.contacttype[?(@.beschrijving == 'Indiener')]" =>  lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
                out[:contact]
            }},

            { "$.opendata.vraagsteller" =>  lambda { |d,o|
                # soort = SCHV 
                # documenttype = Antwoord
                # aggregaattype = Schriftelijke vraag
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
                out[:contact]
            }}
        ],
        legislationResponsible: [
            { "$.contacttype[?(@.beschrijving == 'Bevoegde minister')]" =>  lambda { |d,o|
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
                out[:contact]
            }},
            { "$.opendata.minister" =>  lambda { |d,o|
                # soort = SCHV 
                # documenttype = Antwoord
                # aggregaattype = Schriftelijke vraag
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
                out[:contact]
            }}
        ],
        author: { "$.contacttype[?(@.beschrijving == 'Verslaggever')]" =>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
        contributor:{ "$.contacttype[?(@.beschrijving == 'Spreker')]" =>  lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d["contact"], out, o)
            out[:contact]
        }},
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