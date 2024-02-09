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

            rules_ng.run(RULE_SET_v0_1[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

            rdata[:contributor].uniq! { |c|  c[:name] }
            rdata[:creator] = rdata[:author] 
            
=begin
 ["adult",   ==>>> https://schema.org/contentRating
 "backdrop_path",
 "original_title"  ==>> https://schema.org/name (done)
 "belongs_to_collection", ==>> https://schema.org/MovieSeries
 "budget",
 "imdb_id", 
 "genres",    ==>>> https://schema.org/genre (done)
 "homepage",  
 "popularity",
 "poster_path", ==>>> Url naar poster ??
 "production_companies", ==>>> https://schema.org/productionCompany (done)
 "production_countries",
 "revenue",
 "runtime",
 "spoken_languages",
 "status",
 "tagline",
 "video",
 "vote_average",
 "vote_count",
 "videos",
 "images",
 "imdb_id" ????
 "credits", ==>>> https://schema.org/PerformanceRole (done)
      "actor": {
    "@type": "PerformanceRole",
    "actor": {
      "@type": "Person",
      "name": "Bill Murray"
    },
    "characterName": "Dr. Peter Venkman"
  }
 "external_ids", ==>>>  https://schema.org/identifier  (done)
 "reviews",  ==>>> https://schema.org/Review
 "translations"]
=end
    
=begin
    pp d.keys
    pp "----------------------------"
    # pp rdata
    exit
=end
 
            if rdata[:inLanguage].nil?
                langcode = rdata["@context"].select{ |e| e.is_a?(Hash) && e.has_key?("@language") }[0]["@language"]
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
        name:   '$.original_title', 
        alternateName:  {'$.alternative_titles.titles' =>  lambda { |d,o| 

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_LANGUAGE_SCRIPT[:rs_detect_language_script], d["title"], out, o)
            {
                :@value => d["title"],
                :@language => "#{d["iso_3166_1"].downcase}-#{out[:detect_language_script][0]}"
            }

        }},
        keywords:  { '$.keywords.keywords' =>  lambda { |d,o|
            {
                :@value => d['name'],
                :@langauge => 'en-Latn'
            }
        }},
        genre: {'$.genres' =>  lambda { |d,o| 
            {
                :@value => d['name'],
                :@langauge => 'en-Latn'
            }
        }},
        description:  '$.overview',
        identifier:  {'$.external_ids' =>  lambda { |d,o| 
            rdata = d.map{|k,v|
                unless v.nil?
                    {
                        :@type => "PropertyValue",
                        :@id   => k,
                        :name  => k,
                        :value => v
                    }
                end
            }
            rdata.compact
        }},
        actor:  {'$.credits.cast' =>  lambda { |d,o| 
            if d["known_for_department"] == "Acting"
                {
                    :@type => "PerformanceRole",
                    :actor => {
                        :@type => "Person",
                        :name => d["name"]
                    },
                    :characterName => d["character"]
                }
            end
        }},
        director:  {'$.credits.crew' =>  lambda { |d,o| 
            if d["department"] == "Directing" && d["job"] == "Director"
                {
                    :@type => "Person",
                    :name => d["name"]
                }
            end
        }},
        author:  {'$.credits.crew' =>  lambda { |d,o| 
            if d["known_for_department"] == "Writing" && d["department"] == "Writing" && d["job"] == "Screenplay"
                {
                    :@type => "Person",
                    :name => d["name"]
                }
            end
        }},
        editor:  {'$.credits.crew' =>  lambda { |d,o| 
            if d["known_for_department"] == "Editing" && d["department"] == "Editing" && d["job"] == "Editor"
                {
                    :@type => "Person",
                    :name => d["name"]
                }
            end
        }},   
        contributor: { '$.credits.crew' =>  lambda { |d,o| 
        if ! (
            ( d["department"] == "Directing" && d["job"] == "Director" ) ||
            ( d["known_for_department"] == "Editing" && d["department"] == "Editing" && d["job"] == "Editor" ) || 
            ( d["known_for_department"] == "Writing" && d["department"] == "Writing" && d["job"] == "Screenplay" )
        )
            {
                :@type => "Person",
                :name => d["name"]
            }
        end
        }},
        productionCompany:  {'$.production_companies' =>  lambda { |d,o| 
            #"logo": d["logo_path"]
            {
                :@typ => "Organization",
                :@id => "tmdb_organization_#{d["id"]}",
                :name => d["name"]
            }
        }},
        sameAs:  {'$' =>  lambda { |d,o| 
            "https://www.themoviedb.org/movie/#{d["id"]}" # -star-wars" ???
        }},
        datePublished: {'$.release_date' =>  lambda { |d,o| 
            Time.parse(d).strftime("%Y-%m-%d")
        }},
        releasedEvent:{'$.release_dates.results' =>  lambda { |d,o| 
=begin        
# Can be activated later
            rdata = d["release_dates"].map{ |release_date|
                {
                    "@type": "PublicationEvent",
                    "startDate": Time.parse(release_date["release_date"]).strftime("%Y-%m-%d"),
                    "location": {
                        "@type": "Country",
                        "name": d["iso_3166_1"]
                    }
                }
            }
=end
        }},
        inLanguage: { "$.original_language" =>  lambda { |d,o| 
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
        review: {'$.reviews.results' =>  lambda { |d,o| 
            {
                :@type => "review",
                :@id => "tmdb_review_#{d["id"]}",
                :reviewBody => {
                    :@value =>  d["content"],
                    :@langauge => 'en-Latn'
                },
                :sameAs => d["url"],
                :author => {
                    :@type => "person",
                    :name => d["author"],
                    :alternateName => d["author_details"]["username"]
                },
                :dateCreated => d["created_at"],
                :dateModified => d["updated_at"],
            }
        }}
    } 
}
