#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => [ lambda { |d,o| 
            #o[:actoren] = d["actoren"]
            o[:actoren_vrt] = d["actoren"].select{ |a| a['Codenummer'][0] == "2" }
            o[:actoren_vtm] = d["actoren"].select{ |a| a['Codenummer'][0] == "1" }
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_data], d["thema"], out, o)
            data = out[:data] 
            data
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|

            puts "codenummer #{d["codenummer"]}"
            begin
                date_published = Date.strptime( "20#{d["codenummer"][1..6]}", '%Y%d%m')
            rescue Exception => e 
                begin
                    date_published = Date.strptime( "20#{d["codenummer"][1..6]}", '%Y%m%d')
                rescue Exception => e 
                    puts "Error in codenummer #{d["codenummer"]} / 20#{d["codenummer"][1..6]} (%Y%m%d !!!) => #{e}"
                end
            ensure
                datum = Date.strptime( "#{d['datum']}", '%m/%d/%Y')
                if date_published != datum
                    if date_published.nil? || datum.nil?
                        date_published = datum
                    end
                end
                date_published = date_published.strftime("%Y-%m-%d")
            end

            if d["codenummer"][0] == "1" 
                publisher = o[:publisher][:vtm]  
                INGEST_CONF[:dataset] = {
                    "@id": "ena_vtm",
                    "@type": "Dataset",
                    "name":  "ENA_VTM"
                }
                INGEST_CONF[:genericRecordDesc] = "Entry from Elektronisch Nieuwsarchief - VTM"
            end
            if d["codenummer"][0] == "2"
                publisher = o[:publisher][:vrt]  
                INGEST_CONF[:dataset] =  {
                    "@id": "ena_vrt",
                    "@type": "Dataset",
                    "name":  "ENA_VRT"
                }
                INGEST_CONF[:genericRecordDesc] = "Entry from Elektronisch Nieuwsarchief - VRT"
            end

            rdata = {
                :headline    => Nokogiri::HTML( d["themabeschrijving"] ).text,
                :name        => Nokogiri::HTML( d["themabeschrijving"] ).text,
                :datePublished => date_published,
                :sameAs     => "https://www.nieuwsarchief.be/database/index.php?pg=8&idx=#{d["codenummer"]}&det=1",
                :contentUrl => "https://www.nieuwsarchief.be/database/results.php?idx=#{d["codenummer"]}&movtype=2"
            }
            rdata[:publisher] = publisher

# categories, keywords, topic, entities

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v1_0[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            o[:@id] = out[:basic_schema].to_h[:@id] 
            out.clear

            rules_ng.run(RULE_SET_v1_0[:rs_in_language], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_creator], [ d["journalist1"], d["journalist2"] ], out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_contributor], [ d["mediabron1"], d["mediabron2"] ], out, o)
            
            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_about], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_content_location], d, out, o)

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_keywords], d, out, o)

            rules_ng.run(RULE_SET_v1_0[:rs_duration], d, out, o)

            #actor = o[:actoren].select { |a| a['Codenummer'] == d['codenummer'] || a['codenummer'] == d['codenummer']  }
	    if d["codenummer"][0] == "2"
                actor = o[:actoren_vrt].select { |a| a['Codenummer'] == d['codenummer'] }
            end
            if d["codenummer"][0] == "1"
                actor = o[:actoren_vtm].select { |a| a['Codenummer'] == d['codenummer'] }
            end

            o[:index] = 0
            rules_ng.run(RULE_SET_v1_0[:rs_mentions], actor, out, o)

            rdata.merge!(out.to_h)
            out.clear

            rdata.compact
        }
      }
    },

    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            if d["codenummer"][0] == "1" 
                id = "#{o[:prefixid]}_vtm_#{ d["codenummer"] }"
            end
            if d["codenummer"][0] == "2"
                id = "#{o[:prefixid]}_vrt_#{ d["codenummer"] }"
            end
            unless Iso639[d["language"]].nil? || Iso639[d["language"]].alpha2.to_s.empty?
                language = Iso639[d["language"]].alpha2
            else
                language = INGEST_CONF[:metaLanguage]
            end

            {
                :@id            => id,
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
    rs_mentions: {
        mentions:  { "@" => lambda { |d,o|  
            rdata = nil
            if d["actor_geslacht"].nil?
                description = [d["actor_functie"], d["actor_functie_unclean"]].select! { |o| o.nil? }
                unless description.nil?
                    rdata = {
                        :@type => "Organization",
                        :@id   => "#{  o[:@id] }_Person_#{  o[:index] }",
                        :name => d["actor"],
                        :description => [ 
                            d["actor_functie"],
                            d["actor_functie_unclean"]
                            ],
                        :subjectOf => {
                            :@type    => "VideoObject",
                            :@id      => "#{ o[:@id] }_ACTOR_VIDEO_#{ o[:index] }",
                            :duration => ISO8601::Duration.new( d['duur'].to_i ).to_s,
                            # :inLanguage => o["actor_taal"], # momenteel zit dit niet goed in de data
                            :description => [
                                "Actoraanhetwoord: #{d["Actoraanhetwoord"]}",
                                "actor_spreektijd: #{d["actor_spreektijd"]}",
                            ]
                        } 
                    }
                end
            else
                build_gender = case d["actor_geslacht"].downcase 
                    when "man"; "Male"
                    when "vrouw"; "Female"
                    else "X"
                    end
                occupations = [d["actor_functie"],d["actor_functie_unclean"]].reject! { |o| o.nil? }
                unless occupations.nil?
                    occupations.map! do |occupation|
                        unless occupation.nil?
                            occupation = {
                                :@type => "Occupation",
                                :name =>occupation
                            }
                        end
                    end     
                    rdata = {
                        :@type => "Person",
                        :@id   => "#{  o[:@id] }_Person_#{  o[:index] }",
                        :name => d["actor"],
                        :gender => build_gender,
                        :hasOccupation => occupations,
                        :description => [],
                        :subjectOf => {
                            :@type => "VideoObject",
                            :@id => "#{ o[:@id] }_ACTOR_VIDEO_#{ o[:index] }",
                            :duration => ISO8601::Duration.new( d['duur'].to_i ).to_s,
                            # :inLanguage => a["actor_taal"], # momenteel zit dit niet goed in de data
                            :description => [
                                "Actoraanhetwoord: #{d["Actoraanhetwoord"]}",
                                "actor_spreektijd: #{d["actor_spreektijd"]}",
                            ]
                        } 
                    }
                    unless d["actor_bronnaam"].nil?
                        rdata[:description] << "bronnaam: #{d["actor_bronnaam"]} (Hoe klink deze naam)"
                    end
                    unless d["actor_kleur"].nil?
                        rdata[:description]  << "huidskleur: #{d["actor_kleur"]}"
                    end
                    unless d["actor_handicap"].nil?
                        rdata[:description] <<  "handicap: #{d["actor_handicap"]}"
                    end 
                end
            end
            o[:index] =  o[:index]+1
            rdata
        }}
    },
    rs_in_language: {
        inLanguage: { "@" =>  lambda { |d,o| 
            d = "nl"
            unless Iso639[d].nil? || Iso639[d].alpha2.to_s.empty?
                data = {
                    :@type         => "Language",
                    :@id           => Iso639[d].alpha2,
                    :name          => Iso639[d].name,
                    :alternateName => Iso639[d].alpha2,
                }
            end
            data
        }}
    },
    rs_creator: {
        creator: { "@" =>  [ lambda { |d,o| 
            unless d.nil?
                rdata = {
                    :@type => "Person",
                    :@id   => "#{o[:@id]}_PERSON_#{ o[:index] }",
                    :name  => d
                }
                o[:index] =  o[:index]+1
            end
            rdata
        }] }
    },
    rs_contributor: {
        contributor: { "@" =>  [ lambda { |d,o| 
            unless d.nil?
                rdata = {
                    :@type => "Organization",
                    :@id => "#{o[:@id]}_ORGANIZATION_#{o[:index]}",
                    :name => d
                }
                o[:index] =  o[:index]+1
            end
            rdata
        }] }
    },
    rs_about: {
        about: { "@" =>  lambda { |d,o| 
            rdata = d.select { |k, v| ['thema1','thema2','thema3'].include?(k) }
            rdata = rdata.map { |k, t_id| 

                thema_codes = o[:tv_codebook].select { |p| p['code'] ==  t_id }
                unless thema_codes.empty?
                    {
                        :@type => "Thing",
                        :@id => t_id,
                        :name =>  "#{thema_codes[0]["toplevel"]}: #{thema_codes[0]["title"]}",
                        :description =>  "#{thema_codes[0]["description"]}",
                        :mainEntityOfPage => "https://www.steunpuntmedia.be/wp-content/uploads/2015/04/Codeboek-TV-Nieuwsarchief.pdf"
                    }
                else
                    {
                        :@type => "Thing",
                        :@id => t_id,
                        :name =>  "#{t_id}",
                        :mainEntityOfPage => "https://www.steunpuntmedia.be/wp-content/uploads/2015/04/Codeboek-TV-Nieuwsarchief.pdf",
                    }
                end
            }

            about = []
            about  = about + (d.select { |k,v|  ( k.to_s.match(/^thema_/) && v == "1")   }.keys).map!{ |k| k.gsub(/^thema_/, '')  }
            about  = about + (d.select { |k,v|  ( k.to_s.match(/^Aandacht/) && v == "1") }.keys).map!{ |k| k.gsub(/^Aandacht/, '')  }
           
            about << "Geweld"  unless d['geweld'].nil? || d['geweld'].empty?  || d['geweld'] == " "
            about << "Doden" unless d['doden'].nil? || d['doden'].empty?  || d['doden'] == " "
            
            about.map!{ |t|
                {
                    :@type => "Thing",
                    :@id => t.downcase,
                    :name =>  "#{t}",
                }
            }
        
           
            rdata = rdata + about
            rdata
        } }
    },
    rs_content_location: {
        contentLocation: { "@" =>  lambda { |d,o| 
            rdata = d.select  { |k, v| k.to_s.match(/^land/) && !(v.nil? || v.empty?) }
            rdata =  rdata.values.each_with_index.map do |l, i|  
                {
                    :@type => "Place",
                    # id will be added with write_schema_out => add_all_ids
                    # otherwise duplicate ids could be generated
                    :@id => "#{o[:@id]}_PLACE_#{i}", 
                    :name => l,
                }
            end
            rdata
        } }
    },
    rs_keywords: {
        keywords: { "@" =>  lambda { |d,o| 
            rdata = d['bijhoofdpunten']  == "1" ? ["Hoofdpunt"] : []
            unless d['encyclopedie'].nil?
                rdata.concat( d['encyclopedie'].split(/[,;:]/).map(&:strip) )
            end
            rdata
        } }
    },
    rs_duration: {
        duration: { "@" =>  lambda { |d,o| 
            ISO8601::Duration.new( d['duurtijd'].to_i ).to_s
        } }
    }

}


