#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'language_helpers'

@rule_set_name = "RULE_SET_v0_1"

RULE_SET_v0_1 = {
    version: "0.1",
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
            pp "filename : #{ d["data"].first["primo"].first}_#{d["data"].last["primo"].first}.json"

            "#{ d["data"].first["primo"].first}_#{d["data"].last["primo"].first}.json"
        }}
    },  
    rs_next_value:{
        next_token: { "$" => lambda { |d,o|
            # pp "d[\"from\"]: #{d["from"]}"
            # pp "d[\"step\"]: #{d["step"]}"
            # pp "d[\"count\"]: #{d["count"]}"
            unless d.empty?
                from = d["from"] + d["step"]
                if from < d["count"]
                    rdata = {
                        from: from,
                        step: d["step"],
                        count: d["count"]
                    }
                end
            end
            # pp rdata
            rdata
        }}
    },    
    rs_raw_data:{
        data: { "$" => lambda { |d,o| 
            pp "rs_raw_data  rs_raw_datars_raw_data"
            d["data"]
            }
        },
        metadata: { "$" => lambda { |d,o|
                # De downloadtijd moet worden toegevoegd, aangezien dit later belangrijk kan zijn voor het parsen, 
                # bijvoorbeeld om de periode van de interactionStatistic te bepalen.
                d["metadata"].nil? ? rdata = {} : rdata = d["metadata"]
                rdata[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                rdata[:download_url]  = o[:download_url]
                
                rdata
            }
        },
        error: { "$.error" => lambda { |d,o| 
            d
            }
        }
    },
    rs_id:{
        id:  {'$.primo' =>  lambda { |d,o| 
            d
        }}
    },
    rs_records: {
        records: { "$" => [ lambda { |d,o| 
                
            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
            end
            
            o[:downloadtime] = d["metadata"]["download_time"]

            params  = CGI.parse( URI.parse( d["metadata"]["download_url"] ).query )

            o[:libis_params] = params["libis"].first

            out = DataCollector::Output.new
            rules_ng.run( @rule_set_name[:rs_record], d, out, o )
            
            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
           
            out[:record]
            } ] 
        }
    },
    rs_record: {
        record: { "$.data" => lambda { |d,o|

            rdata = {}

            out = DataCollector::Output.new

            rules_ng.run(@rule_set_name[:rs_id], d, out, o)
            o[:id] = out[:id]

            if  o[:id].nil?
                raise "No id found in d #{d}"
            end
            
            pp "=================================> o[:id] #{o[:id]}"
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h.symbolize_keys)
            out.clear

            o[:contextLanguage] = "nl-Latn"
            o[:contextLanguage] = "en-Latn" if m = /cdi_/.match(o[:id])
                        
            o[:index] = 0
            rules_ng.run(@rule_set_name[:rs_record_type], d, out, o)
            o[:type] = out.data[:@type]

            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.to_h.symbolize_keys)
            out.clear

            if rdata[:sameAs].nil?
                rdata[:sameAs] = "https://lib.is/#{o[:id]}/representation?libis=#{o[:libis_params]}"
            end
 
            #########################################################
            processed_keys = [
                "attribute", # example => ["online_first"] => Primo CDI ?
                "source",
                "type",
                "language",
                "title",
                "format",
                "creator",
                "contributor",
                "creationdate",
                "publisher",
                "mms",
                "addtitle",
                "place",
                "version",
                "subject",
                "keyword", # Primo CDI ?
                "edition",
                "lds17", #ArchiveLevel
                "lds19", #Copies
                "lds33",
                "lds41",
                "lds20", # Full text url ????
                "lds21",
                "lds37", #037 [DOKS], delivery voor Lirias
                "lds25", #Archive creator
                "lds66", #blurb
                "lds50", # Peer Reviewed  => Primo CDI
                "oa", # Open Access  => Primo CDI
                "snippet", # snippet => Primo CDI
                "typographical_details", #typographical_details
                "Genre_local", #Genre_local
                "identifier",
                "local_identifiers",
                "description",
                "ispartof",
                "relation",
                "frequency", #frequency
                "former_title", # former_title
                "additional_info", # additional_info"
                "boundwith", #  ["Link to related record$$9unrelated$$Z9920137610101480"] record mms 9919162490101480
                "language_details", # alma9992732711101480
                "additional_info", # voorbeeld: (ODIS-GEO)10560000005970] alma9992732711101480
                "rights",
                "series",
                "event",
                "local_subjects_ACV",
                "local_subjects_BPB",
                "links_hack",
                "target_audience",
                "enumeration",
                "contents",
                "genre",
                "original_language" # 9918108550101480
            ]

            invalid = d["data"].keys.reject { |k| processed_keys.include?(k) }
            if invalid.empty?
                puts "All keys are processed."
            else
                puts "Missing keys in parsing: #{invalid}"
                pp "INPUT: "
                pp d["data"]
                pp "-------------------------------------------------------------------------------"


                pp "OUTPUT: "
                pp rdata
                exit
            end


            ###########################################################
            # pp rdata
            pp "====================== o[:id] #{o[:id]} ====[ #{rdata[:datePublished]  } ]======> sameAs #{ rdata[:sameAs] }"
            pp "==== identifier : #{ rdata[:identifier] }"
            pp "############################################################################################"

            rdata[:author] = rdata[:creator]

            rdata.compact
            
        } }
    },
    rs_record_type:{
        "@type": {'$.data.type' =>  lambda { |d,o| 
            mapping = {
                "book" => "Book",
                "image" => "ImageObject",
                "video" => "Movie",
                "journals" => "Periodical",
                "archival_material" => "ArchiveComponent",
                "conference_proceeding" =>  "Book",
                "article" => "Article",
                "issue" => "PublicationIssue",
                "audio" => "AudioObject",
                "other" => "CreativeWork",
                "realia" => "CreativeWork", # 9992929537301480 (Paternoster en zilveren borstkruis)

            }
            if mapping[d].nil?
                pp "CREATE MAPPING FOR PRIMO types to schema.org @types [ #{o[:id]}]"
                pp d
                exit
            end
            mapping[d]
        }}
    },
    rs_record_data: {
        source: '$.data.source',
       
        creator: '$.data.creator',
        publication: { '$.data.event' =>  lambda { |d,o|
            {   
                :@type => "PublicationEvent",
                :name => {
                    :@value => d,
                    :@language => o[:contextLanguage]
                }
            }
        }},
        contributor:  '$.data.contributor',
        audience:  { "$.data.target_audience" =>  lambda { |d,o|
            d.gsub("$$T","").gsub("$$","")
        }},
        inLanguage: { "$.data.language" =>  lambda { |d,o|
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
        name: '$.data.title',
        materialExtent: '$.data.format',
        datePublished: {'$.data.creationdate' =>  lambda { |d,o| 
            if dates = /^(\[|-)*(?<year>\d{4})(\]|-|\?|\/)*$/.match(d)
                raw_date = "#{dates[:year]}0101"
            elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)(?<month>\d{2})(\]|-|\?|\/)*$/.match(d)
                raw_date = "#{dates[:year]}#{dates[:month]}01"
            elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)(?<month>\d{2})(\]|-|\?|\/)(?<day>\d{2})$/.match(d)
                raw_date = "#{dates[:year]}#{dates[:month]}#{dates[:day]}"                
            elsif /^(\[)*\d{4}(\]|\s)*-(\s|\[)*\d{4}(\])*$/.match?(d)             
                return nil                
            elsif ["s. d. (sine dato)", "s.d.", "s.d", "[datum van productie onbekend]", "[datum van uitgave onbekend]"].include?(d)
                return nil    
            # Foute invoer ?????  
            elsif ["20. c.","19. c.-20. c."].include?(d)
                return nil                               
            else
                pp "Primo rules: #{ @rule_set_name[:rs_record_data][:datePublished] }: #{d} [#{o[:id]}]"
                exit
            end
            Date.strptime(raw_date, '%Y%m%d').strftime('%Y-%m-%d')
        }},
        _datePublished: '$.data.creationdate',
        datePublished_time_frame:  {'$.data.creationdate' =>  lambda { |d,o| 
            
            start_date = nil
            end_date = nil
            if dates = /^(\[|-)*(?<year>\d{4})(\]|-|\?|\s)*$/.match(d)
                year  = dates[:year].to_i
                start_date = Date.new(year, 1, 1).strftime("%Y%m%d")
                end_date   = (Date.new(year, 12, 31).next_month - 1).strftime("%Y%m%d")
            elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)(?<month>\d{2})(\]|-|\?|\/)*$/.match(d)
                year  = dates[:year].to_i
                month = dates[:month].to_i
                start_date = Date.new(year, month, 1).strftime("%Y%m%d")
                end_date   = (Date.new(year, month, 1).next_month - 1).strftime("%Y%m%d")
            elsif dates = /^(\[)*(?<year>\d{4})(\]|-|\?|\/)(?<month>\d{2})(\]|-|\?|\/)(?<day>\d{2})$/.match(d)
                date = Date.new(dates[:year].to_i, dates[:month].to_i, dates[:day].to_i).strftime("%Y%m%d")
                start_date = date
                end_date   = date            
            elsif dates = /^(\[)*(?<start_year>\d{4})(\]|\s)*-(\s|\[)*(?<end_year>\d{4})(\])*$/.match(d)   
                start_date = Date.new(dates[:start_year].to_i, 1, 1).strftime("%Y%m%d")
                end_date   = (Date.new(dates[:end_year].to_i, 12, 31)).strftime("%Y%m%d")
            else
                pp "datePublished_time_frame creationdatecreationdate #{d}"
                return nil
            end
            {
                "gte": Date.strptime(start_date, '%Y%m%d').strftime('%Y-%m-%d'),
                "lte": Date.strptime(end_date, '%Y%m%d').strftime('%Y-%m-%d')
            }
        }},
        pagination:  {'$.data.enumeration' =>  lambda { |d,o| 
            if (o[:type] == "Article") 
                # This ignores the prefix and just grabs the first two numbers
                pageStart, pageEnd = d.scan(/\d+/).first(2).map(&:to_i)
                unless pageStart && pageEnd 
                    pp "Error in enumeration =>  pagination"
                    exit
                end
                "#{pageStart}-#{pageEnd}"
            else
                pp "Enumeration but type is not article"
                exit
            end
        }},
        pageEnd:  {'$.data.enumeration' =>  lambda { |d,o| 
            if (o[:type] == "Article") 
                # This ignores the prefix and just grabs the first two numbers
                pageStart, pageEnd = d.scan(/\d+/).first(2).map(&:to_i)
                pageEnd
            end
        }},
        pageStart:  {'$.data.enumeration' =>  lambda { |d,o| 
            if (o[:type] == "Article") 
                # This ignores the prefix and just grabs the first two numbers
                pageStart, pageEnd = d.scan(/\d+/).first(2).map(&:to_i)
                pageStart
            end
        }},
        publisher: {'$.data.publisher' =>  lambda { |d,o| 
            {
                :@type => "Organization",
                :@id   => I18n.transliterate( d ).delete(' ').delete('\''),
                :name  => d
            }
        }},
        alternateName: [ '$.data.addtitle', '$.data.former_title'],
        place: '$.data.place',
        version: '$.data.version',
        keywords: [
            '$.data.lds33',
            '$.data.subject',
            '$.data.relation', 
            {'$.data.keyword'  =>  lambda { |d,o| #How To handle lang in Keywords ??? 
                d.split(' ; ').map{ |k|
                    { 
                        :@value => k,
                        :@language => o[:contextLanguage]
                    }
                }
            }}
        ],
        thumbnailUrl: '$.data.lds41',
        sameAs: [
            { '$.data.edition'  =>  lambda { |d,o| 
                if m = /oai:teneo.libis.be:IE/.match(d)
                    "https://lib.is/#{d}/representation"
                elsif m = /lbsn(\d)*1471/.match(d)
                    "https://lib.is/#{d}/representation"
                elsif m = /traj_IE(\d)*/.match(d)
                    "https://lib.is/#{d}/representation" # https://lib.is/traj_IE4627056/representation
                elsif m = /^kadoc_archief(?<scope_id>(\d)*)/.match(d)
                   "https://abs.lias.be/Query/detail.aspx?ID=#{m[:scope_id]}" # return nil to link to Limo with mmsid
                else
                    pp "SameAs: Edition ???"
                    pp d
                    exit
                end
            }}
        ],
        identifier: {"$.data" =>  lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_identifier], d, out, o)
            out.to_h.symbolize_keys.values
        }},
        # thumbnailUrl: "$.data.lds25", #Archive creator
        description:  [
            { '$.data.description'  =>  lambda { |d,o| # lds66 == blurb
                {
                    :@value => d,
                    :@language => o[:contextLanguage]
                }}
            },
            {  '$.data.additional_info' =>  lambda { |d,o| # lds66 == blurb
                {
                    :@value => d,
                    :@language => o[:contextLanguage]
                }}
            },     
            {  "$.data.lds66"  =>  lambda { |d,o| # lds66 == blurb
                pp d.size

                if d.size > 250
                    o[:detectedLanguage] = @icandid_utils.languageDetection("#{d}" , {language_detection_url:  o[:config][:language_detection_url]})
                    o[:detectedLanguage]= detect_language(d, o[:detectedLanguage], o)
                end
                # pp d
                if o[:contextLanguage] != o[:detectedLanguage]
                    pp "data.lds66 : Lang detection ??? oooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooooo"
                    pp o
                    exit
                end
                o[:detectedLanguage] = nil
                
                {
                    :@value => d,
                    :@language => o[:contextLanguage]
                }
            }}
        ],
        isPartOf: [ { '$.data.ispartof' =>  lambda { |d,o|  
                {
                    :@type => "CreativeWork",
                    :name => {
                        :@value => d,
                        :@language => o[:contextLanguage]
                    }
                }
            }},
            { '$.data.series' =>  lambda { |d,o|  
                {
                    :@type => "CreativeWorkSeries",
                    :name => {
                        :@value => d,
                        :@language => o[:contextLanguage]
                    }
                }
            }}
        ],
        abstract: [
            { "$.data.contents"  =>  lambda { |d,o| 
                d
            }}
        ],
        genre: {'$.data.genre' =>  lambda { |d,o|  
            {
              "@value": d,
              "@language": o[:contextLanguage] 
            }
        }},
        # thumbnailUrl: "'$.data.rights", 
        text: { "$.data.lds20"  =>  lambda { |d,o| 
            if m = /https:\/\/resolver.libis.be\/(?<id>[^\s\/]*)\/representation\?fulltext=true/.match(d)
                begin
                    input_options = {
                        number_of_retries: 3,
                        headers: {"Content-Type" => "plain/text", "accept-encoding" => "UTF-8", "Accept" => "plain/text"}
                    }
                    pp "==>>> Start download text from #{d} !!"
                    file_to_check = File.join( File.dirname(o[:file]), "#{  m[:id] }.txt")
                    file =  @icandid_input.download_file_from_uri(url: d , download_path: file_to_check, options: input_options )

                    if file[:content_type] != "plain/text"
                        @logger.warn ("Downloaded file from #{ d } is of type [ #{ file[:content_type] } ]")
                        @logger.warn ("File is saved as #{file_to_check}")
                        return File.read( file_to_check )
                    end
                rescue Exception => e
                    pp "Primo RULES rs_record_data.text Exception: #{e}"
                    if JSON.parse(e&.body)["message"] === "url: Unable to load '#{m[:id]}', teneo: #{m[:id]} not found"
                        return nil
                    end
                    @logger.error (e)
                    exit
                end
            else
                pp "text: lds20  ???"
                pp d
                exit
            end
        }}
    },
    rs_identifier:{
        local_identifiers: {'$.local_identifiers' =>  lambda { |d,o|  
            if  m = /a href=https:\/\/www.wikidata.org\/wiki\/(?<id>[^\s]*)/.match(d)
                {
                    :@type => "PropertyValue",
                    :name => "Wikidata Identifier",
                    :@id => "wikidata_id",
                    :value => m[:id]
                }
            end
        }},
        lds21: {'$.lds21' =>  lambda { |d,o|  
            {
                :@type => "PropertyValue",
                :name => "scopeArchiv Ref Code",
                :@id => "scopeArchiv_ref_code",
                :value => d
            }
        }},
        identifier: {'$.identifier' =>  lambda { |d,o|
            if m = /\$\$C(?<name>[^$]+)\$\$V(?<value>.+)/.match(d)
                {
                    :@type => "PropertyValue",
                    :name =>  m[:name],
                    :@id =>  m[:name].downcase ,
                    :value => m[:value]
                }
            end
        }},
        peer_reviewed: {'$.lds50' =>  lambda { |d,o|
            {
                :@type => "PropertyValue",
                :name =>  "Peer Reviewed",
                :@id =>  "peer_reviewed",
                :value => "Peer Reviewed"
            }
        }},
        oa: {'$.oa' =>  lambda { |d,o|
            {
                :@type => "PropertyValue",
                :name =>  "Open Access",
                :@id =>  "open_access",
                :value => "Open Access"
            }
        }}        

    }
}