#encoding: UTF-8
require 'data_collector'
require "iso639"
require 'ruby-duration'
require_relative 'basic_schema'
require_relative 'language_helpers'

RULE_SET_v0_0_1 = {
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
        records: { "$" => [ lambda { |d,o| 
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v0_0_1[:rs_record], d, out, o)

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
            rules_ng.run(RULE_SET_v0_0_1[:rs_id], d, out, o)
            o[:id] = out[:id]

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            if d["translations"].keys != ["translations"]
                pp "translations keys:"
                pp  d["translations"].keys
                exit
            end
            o[:translations] = d["translations"]["translations"].reject{ |el| el["data"]["title"].nil? }
            o[:original_title] = d["original_title"]
            o[:original_language] = d["original_language"]


            # Remove original_title from alternative_titles 
            d["alternative_titles"]["titles"].reject!{ |el| el["title"].casecmp?(d["original_title"])}
            # necessary for comparing between alternative titles
            o[:alternative_titles] = d["alternative_titles"]["titles"]

            rules_ng.run(RULE_SET_v0_0_1[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

            if rdata[:contributor].is_a?(Array)
                rdata[:contributor].uniq! { |c|  c[:name] } 
            end
            
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
                #langcode = rdata["@context"].select{ |e| e.is_a?(Hash) && e.has_key?("@language") }[0]["@language"]
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
        name:   {'@' =>  lambda { |d,o|
                {
                    :@value => d['original_title'],
                    :@language => detect_language(d["original_title"], d['original_language'], o)  
                }
        }},
        alternateName:  {'$.alternative_titles.titles' =>  lambda { |d,o| 
            translation = o[:translations].select{ |el| el["data"]["title"].casecmp?(d["title"])}
            if translation.size == 1
                lang_code = translation.first&.[]("iso_639_1")
            end
            if translation.size > 1
                pp "More than 1 translation for title #{d["title"]  }"
                lang_code = translation.map{ |t| t["iso_639_1"] }.uniq
                if lang_code.size > 1
                    pp "Filter Translation in regiosn code [#{d["iso_3166_1"]}]"
                    translation = o[:translations].select{ |el| el["data"]["title"].casecmp?(d["title"]) && el["iso_3166_1"] == d["iso_3166_1"].upcase  }
                    if translation.size == 1
                        lang_code = translation.first&.[]("iso_639_1")
                    end
                    if translation.size > 1
                        pp "TRANSLATION : #{translation}"
                        pp "TRANSLATION : #{translation.map { |t| t["iso_639_1"] }.uniq }"
                        pp "Original Title #{o[:original_title] }"
                        pp "Original Language #{o[:original_language]}"
                        pp "Country: #{ ISO3166::Country[ d["iso_3166_1"]] }"
                        pp "Country_languages: #{ ISO3166::Country[ d["iso_3166_1"]].languages }"
                        translation_iso3166_intersection = translation.map { |t| t["iso_639_1"] }.uniq && ISO3166::Country[ d["iso_3166_1"]].languages 
                        if ( translation_iso3166_intersection ).empty?
                            case o[:original_language]
                            when "en"
                                lang_code = o[:original_language]
                            else
                                exit
                            end
                        else
                            lang_code = ( translation_iso3166_intersection ).first
                        end
                    end
                else
                    lang_code = lang_code.first
                end   
            end
            if translation.empty? 
                pp "No translation for alternative title #{ d["title"] }"
                pp "Not possible to detect the language and scripting"
                pp "This input will not be used"
                return nil
            end
            if lang_code.is_a?(Array)
                pp "Multiple lang_codes Found !!!!!!!!!!!!!!!!!!!!!!!!!!!!"
                exit
            end
            {
                :@value => d["title"],
                :@language => detect_language(d["title"], lang_code, o)
            }

        }},
        keywords:  { '$.keywords.keywords' =>  lambda { |d,o|
            {
                :@value => d['name'],
                :@language => 'en-Latn'
            }
        }},
        genre: {'$.genres' =>  lambda { |d,o| 
            {
                :@value => d['name'],
                :@language => 'en-Latn'
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
                :@type => "Organization",
                :@id => "tmdb_organization_#{d["id"]}",
                :name => d["name"]
            }
        }},
        sameAs:  {'$' =>  lambda { |d,o| 
            "https://www.themoviedb.org/movie/#{d["id"]}" # -star-wars" ???
        }},
        datePublished: {'$.release_date' =>  lambda { |d,o| 
            unless d.empty?
                Time.parse(d).strftime("%Y-%m-%d")
            end
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
                :@type => "Review",
                :@id => "tmdb_review_#{d["id"]}",
                :reviewBody => {
                    :@value =>  d["content"],
                    :@language => 'en-Latn'
                },
                :sameAs => d["url"],
                :author => {
                    :@type => "Person",
                    :name => d["author"],
                    :alternateName => d["author_details"]["username"]
                },
                :name => "Review #{Date.parse(d["created_at"]).strftime('%d/%m/%Y')}",
                :dateCreated => Time.parse(d["created_at"]).strftime("%Y-%m-%d"),
                :dateModified => Time.parse(d["updated_at"]).strftime("%Y-%m-%d") 
            }
        }},
        duration: { "$.runtime" =>  lambda { |d,o| 
            #ISO8601::Duration.new( d.to_i ).to_s
            Duration.new(:seconds => d.to_i*60 ).iso8601
        } }
    } 
}
