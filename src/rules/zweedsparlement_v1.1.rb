#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

RULE_SET_v1_1 = {
    version: "1.1",
    rs_next_value: {
        nasta_sida: "$.dokumentlista.@nasta_sida"
    },
    rs_filename:{
        filename: { "$.dokumentlista" => lambda { |d,o| 
            unless (d["dokument"].empty?)
                first_id = d["dokument"].first["id"]
                last_id = d["dokument"].last["id"]
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

            # records downloaded before 18/11/2025 
            # Have no data.ddokumentlista.dokumentlista

            if d.has_key?("data") 
                input = d["data"]["dokumentlista"]["dokument"]
            else
                input = d
            end

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_record], input, out, o)

            out[:record]
        } ] }
    },
    rs_record: {
        record: { "@" => lambda {|d,o|
            rdata = {}

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_id], d, out, o)
            
            o[:id] = "#{o[:ingest_data][:dataset][:@id].downcase}_#{ out[:id] }-00000"

            o[:prefixid] = "#{o[:ingest_data][:dataset][:@id].downcase}_#{ out[:id] }"
            o[:record_id] = out[:id] 
            o[:source_id] = d["id"]
            o[:index] = 0
            

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v1_1[:rs_record_data], d, out, o)
            rdata[:publisher] = o[:default_publisher]

            o[:index] = 0

            rules_ng.run(RULE_SET_v1_1[:rs_contacttype], d["dokintressent"], out, o)
            rules_ng.run(RULE_SET_v1_1[:rs_text], d["filbilaga"], out, o) unless d["filbilaga"].nil?

            unless  out[:text] == out[:all_text]
                pp "AAAAAAAAAAAAAAAAAAAAAAAALLLLLLLLLLLLLLLLLLLLLLLLLLLLLL"
                pp out[:all_text].is_a?(String)
                pp "---------------------------------------------"
                pp out[:text].is_a?(String)
                exit
            end
            rdata.merge!(out.data)
  

            rdata.compact  
        }
      }
    },
    rs_id:{
        id: '$.id'
    },
    rs_record_data: {
        name:  { "$" =>  lambda { |d,o| 
           [ d["titel"], d["undertitel"] ].join(',')
        }},
        description: "$.summary",
        datePublished: "$.publicerad",
        legislationType: { "$" =>  lambda { |d,o| 
            rdata = [ d["typ"], d["subtyp"] ].compact
            type_map = o[:doktyp].select{ |dt|
                d["doktyp"] == dt["doktyp"] &&
                d["typ"] == dt["typ"] &&
                (d["subtyp"] == dt["subtyp"] ||  dt["subtyp"] == "" ||  dt["subtyp"] == "-")
            }.map{ |dt| dt["namn"]}
            if type_map.size != 0
                rdata = type_map.uniq
            end
            rdata
        }},
        legislationPassedBy: { "$" =>  lambda { |d,o| 
            organ = o[:organ].select{ |org| d == org["kod"] }
            organ.map{ |org|
                {
                    :@type => "Organization",
                    :@id   => "#{o[:prefixid]}_ORGANIZATION_#{ org["kod"]  }",
                    :name  => org["namn"],
                    :alternateName  => org["namn_en"],
                    :description  => org["beskrivning"]
                }
            }
        }},       
        inLanguage: { "$" =>  lambda { |d,o| 
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = o[:ingest_data][:metaLanguage]
            end
            
            unless Iso639[language].nil? || Iso639[language].alpha2.to_s.empty?
                {
                    :@type         => "Language",
                    :@id           => Iso639[language].alpha2,
                    :name          => Iso639[language].name,
                    :alternateName => Iso639[language].alpha2,
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
        # sameAs          => "https://data.riksdagen.se/dokumentstatus/#{d["id"]}.json",  
        sameAs: "$.dokument_url_html",
        identifier:  { "$" =>  lambda { |d,o| 
            rdata = []
            unless d["kall_id"].nil?
                rdata << { "@type"=> "PropertyValue", :@id => "data_riksdagen_source_id_#{d["kall_id"]}", :name => "data_riksdagen_source_id", :value => d["kall_id"] }
            end
            unless d["dok_id"].nil?
                rdata << { "@type"=> "PropertyValue", :@id => "data_riksdagen_doc_id_#{d["dok_id"]}", :name => "data_riksdagen_doc_id", :url => d["dokument_url_text"], :value => d["dok_id"] }
            end
        }}
    },
    rs_text: {
        all_text: { "@" => lambda { |d,o| 
            d["fil"].select{ |fil| fil["typ"] == "pdf" }.map{ |fil| fil["namn"] }
            
            source_records_dir = File.dirname( o[:file] ) 
            rdata = []
            
            documents = d['fil'].select { |fil| fil["typ"] == "pdf" } 

            documents.each do |doc| 
                pdf_file = File.join( source_records_dir, doc["namn"] )
                unless File.exist?(pdf_file)
                    input_options = {
                        number_of_retries: 2,
                        headers: {"Content-Type" => "application/pdf", "accept-encoding" => "UTF-8", "Accept" => "application/pdf"}
                    }
                    @logger.warn ("PDF-file does not exits for fulltex extraction: pdf_file: #{pdf_file} file: #{ o[:file] }")
                    @logger.info ("Download file from #{doc["url"]}") 
                    begin
                        file = @icandid_input.download_file_from_uri(url: doc["url"],  download_path: pdf_file, options: input_options )
                    rescue StandardError => e
                        @logger.error "Error Download file from #{doc["url"]}"
                        next
                    end

                    if file[:content_type] != "application/pdf"
                        @logger.warn ("Downloaded file from #{ pdf_file } is of type [ #{ file[:content_type] } ]")
                        @logger.warn ("File is saved as #{pdf_file}")
                        @logger.warn ("Full text extraction for this document format available ????")
                    end
                end

                if File.exist?(pdf_file)
                    pdf_data = File.open( pdf_file )
                    txt_file = "#{File.dirname(pdf_file)}/#{File.basename(pdf_file,'.*')}.txt"
                    tika_fulltext_extraction = @icandid_utils.tikaFullTextExtraction( pdf_data, {file: txt_file } )
                    rdata << tika_fulltext_extraction
                else
                    @logger.warn ("PDF-file does not exits for fulltex extraction: pdf_file: #{pdf_file} file: #{ o[:file] }")
                    sleep 5
                end
            end 

            rdata
        }},
        text: { "$.fil[?(@.typ==\"pdf\")]" => lambda { |doc,o| 
            # d["fil"].select{ |fil| fil["typ"] == "pdf" }.map{ |fil| fil["namn"] }
            
            source_records_dir = File.dirname( o[:file] ) 
            rdata = []
            
            pdf_file = File.join( source_records_dir, doc["namn"] )
            unless File.exist?(pdf_file)
                input_options = {
                    number_of_retries: 2,
                    headers: {"Content-Type" => "application/pdf", "accept-encoding" => "UTF-8", "Accept" => "application/pdf"}
                }
                @logger.warn ("PDF-file does not exits for fulltex extraction: pdf_file: #{pdf_file} file: #{ o[:file] }")
                @logger.info ("Download file from #{doc["url"]}") 
                begin
                    file = @icandid_input.download_file_from_uri(url: doc["url"],  download_path: pdf_file, options: input_options )
                rescue StandardError => e
                    @logger.error "Error Download file from #{doc["url"]}"
                    next
                end

                if file[:content_type] != "application/pdf"
                    @logger.warn ("Downloaded file from #{ pdf_file } is of type [ #{ file[:content_type] } ]")
                    @logger.warn ("File is saved as #{pdf_file}")
                    @logger.warn ("Full text extraction for this document format available ????")
                end
            end

            if File.exist?(pdf_file)
                pdf_data = File.open( pdf_file )
                txt_file = "#{File.dirname(pdf_file)}/#{File.basename(pdf_file,'.*')}.txt"
                tika_fulltext_extraction = @icandid_utils.tikaFullTextExtraction( pdf_data, {file: txt_file } )
                rdata << tika_fulltext_extraction
            else
                @logger.warn ("PDF-file does not exits for fulltex extraction: pdf_file: #{pdf_file} file: #{ o[:file] }")
                sleep 5
            end    
            rdata
        }}
    },
    rs_contacttype: {
        legislationResponsible: {  "$.intressent[?(@.roll == 'undertecknare')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d, out, o)
            out[:contact]
        }},
        author: { "$.intressent[?(@.roll == 'undertecknare')]" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_1[:rs_contact],  d, out, o)
            out[:contact]
        }}
    }, 
    rs_contact: {
        contact: { "$" =>  lambda { |d,o|
            if d["intressent_id"]
                rdata = {
                    :@type => "Person",
                    :@id   => "#{o[:prefixid]}_PERSON_id_#{ d["intressent_id"] }",
                    :name  => "#{d["namn"]}"
                }
            else    
                rdata = {
                    :@type => "Organization",
                    :@id   => "#{o[:@id]}_ORGANIZATION_#{ o[:index] }",
                    :name  => d["namn"]
                }
            end  

            if d["partibet"] 
                partibet = o[:organ].select { |org| org["kod"] == d["partibet"] }.first
                unless partibet.nil?
                    rdata["memberOf"] = {
                        :@type => "Organization",
                        :@id   => "#{o[:prefixid]}_ORGANIZATION__id_parti_#{ d["partibet"]  }",
                        :name  => partibet["namn"],
                        :alternateName  => partibet["namn_en"],
                        :description  => partibet["beskrivning"]
                    }
                end
            end
            rdata
        }}
    }

}
