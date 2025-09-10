#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'
require_relative 'language_helpers'

@rule_set_name = "RULE_SET_v1_0"

RULE_SET_v1_0 = {
    version: "1.0",
    rs_records: {
        records: { "$" => [ lambda { |d,o| 
            #o[:actoren] = d["actoren"]

            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
            end

            o[:actoren_vrt] = d["actoren"].select{ |a| a[ a.keys.find {|k| k.downcase == 'codenummer'} ][0] == "2" }
            o[:actoren_vtm] = d["actoren"].select{ |a| a[ a.keys.find {|k| k.downcase == 'codenummer'} ][0] == "1" }

            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_record], d["thema"], out, o)
            data = out[:record] 
            data
        } ] }
    },
    rs_record: {
        record: { "@" => lambda {|d,o|


            rdata = {}

            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_id], d, out, o)
            o[:id] = out[:id].first

            begin
                raw_date = "20#{d["codenummer"][1..6]}"
                begin
                    date_published = Date.strptime(raw_date, '%Y%d%m')
                rescue
                    begin
                        date_published = Date.strptime(raw_date, '%Y%m%d')
                    rescue => e
                        puts "Error in codenummer #{d["codenummer"]} / #{raw_date} (%Y%m%d !!!) => #{e}"
                    end
                end

                begin
                    datum = Date.strptime(d['datum'], '%m/%d/%Y')
                    if date_published.nil? || date_published != datum
                        date_published ||= datum
                    end
                rescue
                   
                    begin
                        datum = Date.strptime(d['datum'], '%Y-%m-%d')
                        if date_published.nil? || date_published != datum
                            date_published ||= datum
                        end
                    rescue => e
                         puts "d['datum'] ( #{d['datum']} ) not in format'%m/%d/%Y' or '%Y-%m-%d' or invallid"
                    end
                end
                date_published = date_published.strftime('%Y-%m-%d') if date_published
            end

            if d["codenummer"][0] == "1" 
                publisher = o[:publisher][:vtm]  
                o[:ingest_data][:dataset] = {
                    "@id": "ena_vtm",
                    "@type": "Dataset",
                    "name":  "ENA_VTM"
                }
                o[:ingest_data][:genericRecordDesc] = "Entry from Elektronisch Nieuwsarchief - VTM"
            end
            if d["codenummer"][0] == "2"
                publisher = o[:publisher][:vrt]  
                o[:ingest_data][:dataset] =  {
                    "@id": "ena_vrt",
                    "@type": "Dataset",
                    "name":  "ENA_VRT"
                }
                
                o[:ingest_data][:genericRecordDesc] = "Entry from Elektronisch Nieuwsarchief - VRT"
            end
            
            o[:prefixid] = "#{o[:ingest_data][:prefixid]}_#{  o[:ingest_data][:dataset][:@id].downcase }_#{o[:id]}".gsub("_ena_","_ENA_")

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rdata["@id"] = o[:prefixid]

            rdata.merge!({
                :headline    => Nokogiri::HTML( d["themabeschrijving"] ).text,
                :name        => Nokogiri::HTML( d["themabeschrijving"] ).text,
                :publisher   => publisher,
                :datePublished => date_published,
                :sameAs     => "https://www.nieuwsarchief.be/database/index.php?pg=8&idx=#{d["codenummer"]}&det=1",
                :contentUrl => "https://www.nieuwsarchief.be/database/results.php?idx=#{d["codenummer"]}&movtype=2"
            })

            # categories, keywords, topic, entities
            o[:index] = 0
            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.to_h)
            out.clear

            #actor = o[:actoren].select { |a| a['Codenummer'] == d['codenummer'] || a['codenummer'] == d['codenummer']  }
	        if d["codenummer"][0] == "2"
                actor = o[:actoren_vrt].select { |a| (a['Codenummer'] || a['codenummer'] ) == d['codenummer'] }
            end
            if d["codenummer"][0] == "1"
                actor = o[:actoren_vtm].select { |a| (a['Codenummer'] || a['codenummer'] ) == d['codenummer'] }
            end

            o[:index] = 0
            rules_ng.run(@rule_set_name[:rs_mentions], actor, out, o)

            rdata.merge!(out.to_h)
            out.clear

            rdata.compact
        }
      }
    },
    rs_id:{
        id:  {'$.codenummer' =>  lambda { |d,o|
            d
        }}
    },
    rs_record_data:{
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
        }},
        creator: [
            { "$.journalist1" =>  lambda { |d,o| 
                unless d.nil?
                    rdata = {
                        :@type => "Person",
                        :name  => d
                    }
                end
                rdata
            } },
            { "$.journalist2" =>  lambda { |d,o| 
                unless d.nil?
                    rdata = {
                        :@type => "Person",
                        :name  => d
                    }
                end
                rdata
            } }
        ],
        contributor: [
            { "$.mediabron1" =>  lambda { |d,o| 
                unless d.nil?
                    rdata = {
                        :@type => "Organization",
                        :name => d
                    }
                    o[:index] =  o[:index]+1
                end
                rdata
             } },
             { "$.mediabron2" =>  lambda { |d,o| 
                unless d.nil?
                    rdata = {
                        :@type => "Organization",
                        :name => d
                    }
                    o[:index] =  o[:index]+1
                end
                rdata
             } },
        ],
        about: { "@" =>  lambda { |d,o| 
            about = d.select { |k, v| ['thema1','thema2','thema3'].include?(k) && v.to_i != 0}

            about = about.map { |k, t_id| 

                thema_codes = o[:tv_codebook].select { |p| p['code'].to_i ==  t_id.to_i }

                unless thema_codes.empty?
                    {
                        :@type => "Thing",
                        :@id =>  t_id.to_i,
                        :name =>  "#{thema_codes[0]["toplevel"]}: #{thema_codes[0]["title"]}",
                        :description =>  "#{thema_codes[0]["description"]}",
                        :mainEntityOfPage => "https://www.steunpuntmedia.be/wp-content/uploads/2015/04/Codeboek-TV-Nieuwsarchief.pdf"
                    }
                else
                    {
                        :@type => "Thing",
                        :@id => t_id.to_i,
                        :name =>  "#{t_id.to_i}",
                        :mainEntityOfPage => "https://www.steunpuntmedia.be/wp-content/uploads/2015/04/Codeboek-TV-Nieuwsarchief.pdf",
                    }
                end
            }

            # Standard JSONPath does not support filtering based on attribute names (keys)
            # regex or starts_with, ... is only available for values
            
            about  = about + (d.select { |k,v|  ( k.to_s.match(/^thema_/) && v.to_i == 1)   }.keys).map!{ |k| k.gsub(/^thema_/, '')  }
            about  = about + (d.select { |k,v|  ( k.to_s.match(/^Aandacht/) && v.to_i == 1) }.keys).map!{ |k| k.gsub(/^Aandacht/, '')  }
           
            # 
            about << "Geweld"  unless d['geweld'].nil? || d['geweld'].empty?  || d['geweld'] == " "
            about << "Doden" unless d['doden'].nil? || d['doden'].empty?  || d['doden'] == " "
            
            about.map!{ |t|
                if t.is_a?(String)
                {
                    :@type => "Thing",
                    :@id => t.downcase,
                    :name =>  "#{t}",
                }
                else
                    t
                end
            }
            about
        } },
        contentLocation: { "@" =>  lambda { |d,o| 
        
            # Standard JSONPath does not support filtering based on attribute names (keys)
            # regex or starts_with, ... is only available for values
            location = d.select  { |k, v| k.to_s.match(/^land/) && !(v.nil? || v.empty?) }
            location =  location.values.each_with_index.map do |l, i|  
                {
                    :@type => "Place",
                    # id will be added with write_schema_out => add_all_ids
                    # otherwise duplicate ids could be generated
                    :@id => "#{o[:@id]}_PLACE_#{i}", 
                    :name => l,
                }
            end
            location
        } },

        keywords: [
            { "$.bijhoofdpunten" =>  lambda { |d,o| 
                if d.to_i == 1
                    "Hoofdpunt"
                end
            } },
            { "$.encyclopedie" =>  lambda { |d,o|
                unless d.nil?
                    d.split(/[,;:\n]/).map(&:strip) 
                end
            } }
        ],
        duration: { "$.duurtijd" =>  lambda { |d,o| 
            ISO8601::Duration.new( d.to_i ).to_s
        } }
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
    }
=begin    
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
    }  
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
=end
}