#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'detect_language_script'

RULE_SET_v0_1 = {
    version: "0.1",
    rs_next_value: {
    },
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
                unless d["id"].empty?
                    "tmdb_#{d["id"]}.json"
                end
        } }
    },
    rs_raw_data:{
        data: { "$" => lambda { |d,o| 
               d
            }
        },
        error: { "$" => lambda { |d,o| 
            nil
            }
        }
    },
    rs_records: {
        records: { "$." => [ lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v0_1[:rs_record], d, out, o)

            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
            
            out[:record]

        } ] }
    },
    rs_record: {
        record: { "$" => lambda { |d,o| 

            rdata = {}

            #pp d
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v0_1[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear


=begin
[
 "id",   => id
 "title", => name
 "originalTitle", => alternateName
 "fullTitle", => alternateName
 "type",
 "year",
 "image",
 "releaseDate", => datePublished
 "plot", => description
 "plotLocal",
 "plotLocalIsRtl",
 "awards",
 "directors", 
 "directorList",=> director
 "writers",
 "writerList", => author
 "stars",
 "starList",
 "actorList", => actor
 "fullCast",  => contributor / editor
    ["imDbId", "title", "fullTitle", "type", "year", "directors", "writers", "actors", "others", "errorMessage"]
 "genres", 
 "genreList",=> genre
 "companies",
 "companyList",  => productionCompany
 "countries",
 "countryList",
 "languages", => inLanguage
 "languageList",
 "ratings",
 "wikipedia",
 "posters",
 "images", => associatedMedia
 "trailer",
 "boxOffice",
 "keywords", => keywords
 "keywordList", => keywords
 "similars",
 "errorMessage",
 "reviews", => review
 "metacriticreviews"
]
=end
# --------------------------------


            rules_ng.run(RULE_SET_v0_1[:rs_record_data], d, out, o)
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
        id:  {'$.id' =>  lambda { |d,o| 
            d
        }}
    },
    rs_record_data: {
        name:   '$.title', 
        alternateName:  [
            {'$.originalTitle' =>  lambda { |d,o| 
                unless d.nil? || d.empty?
                    out = DataCollector::Output.new
                    rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], d, out, o)
                    {
                        :@value => d,
                        :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
                    }
                end
            }},
            {'$.fullTitle' =>  lambda { |d,o| 
                unless d.nil? || d.empty?
                    out = DataCollector::Output.new
                    rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], d, out, o)
                    {
                        :@value => d,
                        :@language => "#{ o[:ingest_data][:metaLanguage].downcase }-#{out[:detect_language_script][0]}"
                    }
                end
            }},
        ],
        keywords:  { '$.keywordList' =>  lambda { |d,o|
            {
                :@value => d,
                :@language => 'en-Latn'
            }
        }},
        genre: {'$.genreList' =>  lambda { |d,o| 
            {
                :@value => d["value"],
                :@language => 'en-Latn'
            }
        }},
        description: [
            { '$.plot' =>  lambda { |d,o| 
                {
                    :@value => d,
                    :@language => 'en-Latn'
                }
            }},
            {'$.wikipedia.plotFull.plainText' =>  lambda { |d,o| 
                {
                    :@value => d,
                    :@language => 'en-Latn'
                }
            }}
        ],
        actor:  {'$.actorList' =>  lambda { |d,o| 
            {
                :@type => "PerformanceRole",
                :actor => {
                    :@type => "Person",
                    :name => d["name"],
                    :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{d["id"]}",
                    :url => "https://www.imdb.com/name/#{d["id"]}"
                },
                :characterName => d["asCharacter"]
            }

        }},
        director:  {'$.directorList' =>  lambda { |d,o| 
            {
                :@type => "Person",
                :name => d["name"],
                :url => "https://www.imdb.com/name/#{d["id"]}",
                :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{d["id"]}"
            }
        }},
        author:  {'$.writerList.' =>  lambda { |d,o| 
            {
                :@type => "Person",
                :name => d["name"],
                :url => "https://www.imdb.com/name/#{d["id"]}",
                :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{d["id"]}"
            }
        }},
        editor:  {'$.fullCast.others' =>  lambda { |d,o| 
            if d["job"] == "Editing by"
                d["items"].map { |p| 
                    {
                        :@type => "Person",
                        :name => p["name"],
                        :url => "https://www.imdb.com/name/#{p["id"]}",
                        :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{p["id"]}",
                    }
                }
            end
        }},   
        producer:  {'$.fullCast.others' =>  lambda { |d,o| 
            if d["job"] == "Produced by"
                d["items"].map { |p| 
                    {
                        :@type => "Person",
                        :name => p["name"],
                        :url => "https://www.imdb.com/name/#{p["id"]}",
                        :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{p["id"]}",
                    }
                }
            end
        }}, 

        
        musicBy:  {'$.fullCast.others' =>  lambda { |d,o| 
        if d["job"] == "Music by"
            d["items"].map { |p| 
                {
                    :@type => "Person",
                    :name => p["name"],
                    :url => "https://www.imdb.com/name/#{p["id"]}",
                    :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{p["id"]}",
                }
            }
        end
        }}, 
        contributor: { '$.fullCast.others' =>  lambda { |d,o| 
            if ! ["Editing by", "Music by", "Produced by"].include?(d["job"])
                d["items"].map { |p| 
                    {
                        :@type => "Person",
                        :name => p["name"],
                        :url => "https://www.imdb.com/name/#{p["id"]}",
                        :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_#{p["id"]}",
                        :hasOccupation => {
                            :@type => "Occupation",
                            :name => d["job"]
                        }
                    }
                }
            end
        }},
        productionCompany:  {'$.companyList' =>  lambda { |d,o| 
            #"logo": d["logo_path"]
            {
                :@type => "Organization",
                :@id => "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:provider][:@id].downcase }_organization_#{d["id"]}",
                :name => d["name"]
            }
        }},
        sameAs:  {'$' =>  lambda { |d,o| 
            "https://www.imdb.com/title/#{d["id"]}" # -star-wars" ???
        }},
        datePublished: {'$.releaseDate' =>  lambda { |d,o| 
            unless d.nil?
                Time.parse(d).strftime("%Y-%m-%d")
            end
        }},
        inLanguage: { "$.languages" =>  lambda { |d,o| 
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
        review: {'$.reviews' =>  lambda { |d,o|
            unless d.nil? || d.empty? 
                {
                    :@type => "Review",
                    :@id => "imbd_review_#{d["reviewLink"].split('/').last}",
                    :reviewBody => {
                        :@value =>  d["content"],
                        :@language => 'en-Latn'
                    },
                    :sameAs => d["reviewLink"],
                    :author => {
                        :@type => "Person",
                        :name => d["username"],
                        :url => d["userUrl"]
                    },
                    :name => d["title"],
                    :dateCreated => Time.parse(d["date"]).strftime("%Y-%m-%d")

                }
            end
        }},
        associatedMedia: {'$.images.items' =>  lambda { |d,o| 
            {
                :@type => "MediaObject",
                :name => d["title"],
                :url => d["image"]
            }
        }},
        trailer: {'$.trailer' =>  lambda { |d,o| 
            unless d.nil? || d.empty?
                {
                    :@type => "VideoObject",
                    :name => d["fullTitle"],
                    :description => d["videoDescription"],
                    :thumbnailUrl => d["thumbnailUrl"],
                    :url => d["link"],
                    :embedUrl => d["linkEmbed"]
                }
            end
        }}
    } 
}
