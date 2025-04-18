#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

# @tiktokusers = {}
# prevent request to tiktok API if user already exists in this Hash

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

            puts "RULES RULES RULES records o[:tiktokusers] #{o[:tiktokusers].keys.size}"
            @tiktokusers = o[:tiktokusers]

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v0_1[:rs_record], d, out, o)

            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
           
            out[:record]
        } ] },
        options: { "$.data." => [ lambda { |d,o| 
                o[:tiktokusers] = @tiktokusers
                o
        }] }
    },
    rs_record: {
        record: { "$.videos" => lambda { |d,o| 

            rdata = {}

            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v0_1[:rs_id], d, out, o)
            o[:id] = out[:id].first

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            rules_ng.run(RULE_SET_v0_1[:rs_record_data], d, out, o)
            rdata.merge!(out.data)
            rdata[:creator] = rdata[:author] = rdata[:sender] 

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
        text:   '$.voice_to_text',
        name:   '$.video_description',
        sender: {'$.username' => lambda { |d,o| 
            user = @tiktokusers[d]

            #pp "search user in @tiktokusers #{d}"
            #pp "user: #{user}"

            if user.nil?
                user = { 
                    :@id         => d,
                    :identifier => {
                            :@type  => "PropertyValue",
                            :name   => "verified",
                            :@id    => "tiktok_verified_true",
                        },
                    :@type       => "Person",
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

                user[:description] = "Couldn't find this account [#{d}]"
                user[:name] = "unknown for #{d}"

                use_screenscrape = true

                unless use_screenscrape
                    url = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"

                    # pp "Bearer token: #{o[:config][:auth][:bearer_token]}"
                    # pp "username: #{d}"
                    
                    options = {
                        bearer_token: o[:config][:auth][:bearer_token],
                        method: "POST",
                        body:   JSON.generate( {"username": d } )
                    }
                    begin
                        icandid_input = IcandidCollector::Input.new( :icandid_config => @icandid_config )
                        user_data = icandid_input.collect_data_from_uri(url: url,  options: options )

                    rescue RuntimeError => e
                        if error_400 = e.message.match(/^Unable to process received status code = 400 error=(.*)/i).captures.first
                            error_400 = JSON.parse( error_400.strip )
                            pp error_400["error"]["code"]
                            pp error_400["error"]["message"]
                            user[:description] = error_400["error"]["message"]
                        end
                    rescue StandardError => e
                        pp "Error: #{e.message}"
                        pp e.message
                        halt
                        pp e.backtrace
                        exit
                    end
                end

                if use_screenscrape
                    http = HTTP
                    url = "https://www.tiktok.com/@#{d}"
                    http_response = http.follow.get(url, {})
                    data = http_response.body.to_s
                    raw_data = Nokogiri::HTML(data)
                    jdata = JSON.parse( raw_data.xpath("/html/body/script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']").text )
                    # pp jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]["user"].keys
                    # pp "shareMeta => #{jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["shareMeta"]}"
                    
                    if jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"].has_key?("userInfo")
                        screenscrape_user_data = jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
                        user_data = {
                            "data" => {
                                "avatar_url"      => screenscrape_user_data["user"]["avatar_url"],
                                "display_name"    => screenscrape_user_data["user"]["nickname"],
                                "is_verified"     => screenscrape_user_data["user"]["verified"],
                                "bio_description" => screenscrape_user_data["user"]["signature"],   
                                "likes_count"     => screenscrape_user_data["stats"]["heartCount"],
                                "video_count"     => screenscrape_user_data["stats"]["videoCount"],
                                "following_count" => screenscrape_user_data["stats"]["followingCount"],
                                "follower_count"  => screenscrape_user_data["stats"]["followerCount"]
                            },
                            "error" => {
                                "code" => "ok"
                            }
                        }
                    else
                        user_data = {
                            "error" => {
                                "code" => "user not found ?"
                            }
                        }
                    end


                end

                unless user_data.nil? || user_data["data"].nil? || user_data["error"]["code"] != "ok"
                    user[:identifier][:value] = user_data["data"]["is_verified"]
                    user[:description] = user_data["data"]["bio_description"]
                    user[:name] = user_data["data"]["display_name"]
                    user[:logo] = user_data["data"]["avatar_url"]

                    if user_data["data"]["is_verified"]
                        user[:memberOf][:roleName] << "verified user"
                    end
                    # => count : It is a snapshots - date must be mentions if added to the data
                    user[:interactionStatistic] = []
                    unless user_data["data"]["likes_count"].nil?
                        user[:interactionStatistic] << { 
                            :@type                => "InteractionCounter",
                            :interactionType      => "https://schema.org/LikeAction",
                            :userInteractionCount => user_data["data"]["likes_count"],
                            :endTime              => Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                        }
                    end
                    unless user_data["data"]["video_count"].nil?
                        user[:interactionStatistic] <<  { 
                            :@type                => "InteractionCounter",
                               :interactionType      => "https://schema.org/CommunicateAction",
                            :userInteractionCount => user_data["data"]["video_count"],
                            :endTime              => Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                        }
                    end
                    unless user_data["data"]["follower_count"].nil?
                        user[:interactionStatistic] <<  { 
                            :@type                => "InteractionCounter",
                            :interactionType      => "https://schema.org/FollowAction",
                            :userInteractionCount => user_data["data"]["follower_count"],
                            :endTime              => Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                        }
                    end                    
                end  

                @tiktokusers[d] = user              

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
            #pp "datePublished: #{d}"
            #pp "datePublished: #{ Time.at(d).strftime("%Y-%m-%d") }"
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
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/ViewAction",
                    :userInteractionCount => d["view_count"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end
            unless d["like_count"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/LikeAction",
                    :userInteractionCount => d["like_count"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end
            unless d["share_count"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/ShareAction",
                    :userInteractionCount => d["share_count"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end
            unless d["comment_count"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/CommentAction",
                    :userInteractionCount => d["comment_count"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
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
