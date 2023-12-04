#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

@tiktokusers = {}

RULE_SET_v0_1 = {
    version: "0.1",
    rs_next_value: {
        search_id: { "$.data.search_id" => [ lambda { |d,o| 
                d
            }]
        },
        cursor: { "$.data.cursor" => [ lambda { |d,o| 
                d
            }]
        },
        has_more: { "$.data.has_more" => [ lambda { |d,o| 
                    d
            }]
        }
    },
    rs_filename:{
        filename: { "$.data" => lambda { |d,o| 
                unless d["videos"].empty?
                    "tiktok_#{d["videos"].first["id"]}_#{d["videos"].last["id"]}.json"
                end
            }
        }
    },
    rs_raw_data:{
        data: { "$.data" => lambda { |d,o| 
               d
            }
        },
        error: { "$.error" => lambda { |d,o| 
            d
            }
        }
    },
    rs_records: {
        records: { "$.data." => [ lambda { |d,o| 
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
        record: { "$.videos" => lambda { |d,o| 

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

=begin
  "like_count"=>22,
 "music_id"=>7291785472958351362,
 "username"=>"psbelgique",
 "video_description"=>
  "Nous sommes pour l’interdiction de la fessée et de toute forme de violence physique ou psychologique  envers les enfants, malheureusement le MR bloque.",
 "hashtag_names"=>[],
 "create_time"=>1700591646,
 "id"=>7303985428648987936,
 "region_code"=>"BE",
 "share_count"=>0,
 "view_count"=>6,
 "voice_to_text"=>
  "Est-ce qu'on a vraiment envie d'être le dernier pays d'Europe à euh. Ne pas interdire la violence comme méthode éducative ? Nous avons déposé 1 proposition justement pour interdire aux parents le recours systématique à la violence, qu'elle soit psychologique ou physique des parents envers leur enfant. Alors évidemment, dans la majorité, certains ne l'entendent pas de cette oreille. Il y a pas de majorité à l'heure actuelle pour le voter.",
 "comment_count"=>1
=end

        description: '$.voice_to_text',
        name:        '$.video_description',
        sender:     {'$.username' => lambda { |d,o| 

            user = @tiktokusers[d]

            if user.nil?
                url = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"
                
                options = {
                    bearer_token: o[:auth][:bearer_token],
                    method: o[:method],
                    body:   JSON.generate( {"username": d } )
                }
   
                icandid_input = IcandidCollector::Input.new( :icandid_config => @icandid_config )
                user_data = icandid_input.collect_data_from_uri(url: url,  options: options )
=begin
{"data"=>
  {"bio_description"=>
    "Créons le monde de demain 🌍\n" +
    "➕ Juste ⚖️ ➕ Solidaire 🤝 ➕ Durable 🌱\n" +
    "#psbelgique",
   "display_name"=>"Parti Socialiste 🌹🇧🇪",
   "follower_count"=>9276,
   "following_count"=>48,
   "is_verified"=>false,
   "likes_count"=>84782,
   "video_count"=>339,
   "avatar_url"=>
    "https://p77-sign-va.tiktokcdn.com/tos-maliva-avt-0068/c3a9e517a0067754131a1a51399534ca~c5_168x168.jpeg?x-expires=1701432000&x-signature=2FFBehiAmlcnOszxhGXjfSwRVGo%3D"},
 "error"=>
  {"code"=>"ok",
   "message"=>"",
   "log_id"=>"202311291237181C7EF703CA61E7020BB9"}}
=end
                unless user_data.nil? || user_data["data"].nil? || user_data["error"]["code"] != "ok"
                    user = { 
                        :id         => d,
                        :identifier => {
                                :@type  => "PropertyValue",
                                :name   => "verified",
                                :@id    => "tiktok_verified_true",
                                :value  => user_data["data"]["is_verified"]
                            },
                        :@type       => "Organization",
                        :description => user_data["data"]["bio_description"],
                        :name        => user_data["data"]["display_name"],
                        :logo        => user_data["data"]["avatar_url"],
                        :sameAs      => "https://www.tiktok.com/@#{d}",
                        :memberOf    => {
                            :@type => "OrganizationRole",
                            :roleName => ["user"],
                            :@id => "iCANDID_tiktok_PERSON_ORGANIZATION_ROLE_#{d.upcase}",
                            :memberOf => {
                                :@type => "Organization",
                                :name => "TikTok",
                                :@id => "iCANDID_ORGANIZATION_TIKTOK"
                            }
                        }
                    } 
                    if user_data["data"]["is_verified"]
                        user[:roleName][:memberOf][:roleName] << "verified user"
                    end
=begin        
                    # => count : It is a snapshots - date must be mentions if added to the data
                    user_data["interactionStatistic"] = []
                    unless d["likes_count"].nil?
                        user_data["interactionStatistic"] <<  { 
                            "@type": "InteractionCounter",
                            "interactionType": "https://schema.org/LikeAction",
                            "userInteractionCount": d["likes_count"]
                        }
                    end
=end 

                    @tiktokusers[d] = user
                end                
            end
            user
        }},
        keywords:    '$.hashtag_names',
        identifier:  {'@' =>  lambda { |d,o| 
            unless d["music_id"].nil?
                {
                    :@type => "PropertyValue",
                    :@id   => "music_id",
                    :name  => "music_id",
                    :value => d["music_id"]
                }
            end
        }},
        sameAs:  {'$' =>  lambda { |d,o| 
            "https://www.tiktok.com/@#{d["username"]}/video/#{d["id"]}"
        }},
        datePublished: {'$.create_time' =>  lambda { |d,o| 
            Time.at(d).strftime("%Y-%m-%d")
        }},
        associatedMedia: { "$.music_id" => lambda { |d,o| 
            {
                :@type         => "AudioObject",
                :@id           => "#{o[:prefixid]}_MEDIA_#{d}"
            }
        }},
        interactionStatistic:  {'@' =>  lambda { |d,o| 
            rdata = []
            unless d["view_count"].nil?
                rdata <<  { 
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/ViewAction",
                    "userInteractionCount": d["view_count"]
                }
            end
            unless d["like_count"].nil?
                rdata <<  { 
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/LikeAction",
                    "userInteractionCount": d["like_count"]
                }
            end
            unless d["share_count"].nil?
                rdata <<  { 
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/ShareAction",
                    "userInteractionCount": d["share_count"]
                }
            end
            unless d["comment_count"].nil?
                rdata <<  { 
                    "@type": "InteractionCounter",
                    "interactionType": "https://schema.org/CommentAction",
                    "userInteractionCount": d["comment_count"]
                }
            end            
            rdata 
        } },
        locationCreated: { "$.region_code" =>  lambda { |d,o| 
            d
        }},
        inLanguage: { "$" =>  lambda { |d,o| 
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
        }}
    } 
}
