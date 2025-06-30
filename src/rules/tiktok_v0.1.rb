#encoding: UTF-8
require 'data_collector'
require "iso639"
require "countries"
require_relative 'basic_schema'
require_relative 'language_helpers'

# @tiktokusers = {}
# prevent request to tiktok API if user already exists in this Hash
# Only used if screenscraping is inactive (screenscraping is default active)

@tiktokusers = {}

def get_comment_data_from_file( file )
    icandid_input = IcandidCollector::Input.new( :icandid_config => {} )
    comment_data = icandid_input.collect_data_from_uri(url: "file://#{file}",  options: {} )   

    return comment_data
end

def get_comment_data_from_uri( d,o )

    item_id    = d["aweme_id"]
    comment_id = d["comment_id"]

    count  = o[:count] || "50"
    cursor = o[:cursor] || "0"
    number_of_retries     = ( o[:number_of_retries] || 0 ).to_i
    max_number_of_retries = ( o[:max_number_of_retries] || 3 ).to_i

    odinId    = 9999
    url_params = {
        "WebIdLastTime" => "test",
        "aid" => "1988",
        "app_language" => "en-US",
        "app_name" => "tiktok_web",
        "browser_language" => "en",
        "browser_name" => "Browy",
        "browser_online" => "true",
        "browser_platform" => "plat",
        "browser_version" => "browser",
        "channel" => "tiktok_web",
        "cookie_enabled" => "true",
        "data_collection_enabled" => "false",
        "device_id" => "9999",
        "device_platform" => "web_pc",
        "focus_state" => "true",
        "from_page" => "video",
        "history_len" => "2",
        "is_fullscreen" => "false",
        "is_page_visible" => "true",
        "os" => "windows",
        "priority_region" => "",
        "referer" => "",
        "region" => "BE",
        "screen_height" => "1200",
        "screen_width" => "1920",
        "tz_name" => "Europe%2FBrussels",
        "user_is_login" => "true",
        "webcast_language" => "en-US",
        "X-Bogus" => "Bogus"
    }

    tokens = [('a'..'z'), ('A'..'Z')].map(&:to_a).flatten

    msToken = (0...50).map { tokens[rand(o.length)] }.join
    msToken = "#{msToken}_#{rand(1000..9999)}"
    x_gnarly = (0...50).map { tokens[rand(o.length)] }.join
    x_gnarly = "#{x_gnarly}_#{rand(1000..9999)}"

    url_params["aweme_id"] = item_id
    url_params["item_id"] = item_id
    url_params["comment_id"] = comment_id
    url_params["count"] = count
    url_params["cursor"] = cursor
    url_params["odinId"] = odinId
    url_params["msToken"] = msToken         
    url_params["X-Gnarly"] = x_gnarly

    url_query_string = URI.encode_www_form(url_params)

    if comment_id.nil?
        comments_url = "https://www.tiktok.com/api/comment/list/?#{url_query_string}"
    else
        #url_params.delete("aweme_id")
        #url_query_string = URI.encode_www_form(url_params)
        comments_url = "https://www.tiktok.com/api/comment/list/reply/?#{url_query_string}"
    end


    http = HTTP
    http_response = http.follow.get(comments_url)
    comment_data = JSON.parse( http_response.body.to_s )

    #pp comment_data.keys
    #pp "comment_data[cursor]: #{comment_data["cursor"]}"
    #pp "comment_data[has_more]: #{comment_data["has_more"]}"
    #pp "comment_data[total]: #{comment_data["total"]}"
    #pp "comment_data[status_code]: #{comment_data["status_code"]}"
    #pp "comment_data[status_msg]: #{comment_data["status_msg"]}"

    # Tiktok contains share_info.acl.code == 1 if share info failed
    # Retry the request after 10 second
    # After 5 retries try to create to share_info based on aother fields in the response

    #"share_info": {
    #    "acl": {
    #        "code": 1,
    #        "extra": "{\"is_share_handler_failed\":\"1\"}"
    #    },
    #    "desc": "",
    #    "title": "",
    #    "url": ""
    #},


    if comment_data["comments"].nil?
        @logger.warn("!! !! !! !! !! No comments retrieved for #{d} on URL  #{comments_url} ")
        comment_data["comments"] = []
        comment_data[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
        return comment_data
    end

    if comment_data["comments"].nil? || comment_data["comments"].empty?
        @logger.warn("!! !! !! !! !! No comments retrieved for #{d} on URL  #{comments_url} ")
        comment_data["comments"] = []
        comment_data[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
        return comment_data
        # raise "empty comments Array for #{comments_url}"
    end

    is_share_handler_failed = comment_data["comments"].select do |comment| 
        comment["share_info"]["acl"]["code"] > 0 && comment["share_info"]["acl"]["desc"]  == ""
    end

    if is_share_handler_failed.size > 0
        @logger.warn("Comments not retrieved properly (missing share_info)")
        if number_of_retries < max_number_of_retries
            @logger.warn("Sleep 5 second and retry" ) 
            sleep 5
            o[:number_of_retries] = number_of_retries + 1
            comment_data = get_comment_data_from_uri( d, o )
        else
            @logger.error("Retried #{number_of_retries} times." ) 
            @logger.error("Comments_url #{comments_url}" ) 
            raise "Retried #{number_of_retries} times.\n Comments_url #{comments_url}"
            exit
        end

    end

    if comment_data["has_more"] != 0 && comment_data["total"] > comment_data["cursor"]
        o[:cursor] = comment_data["cursor"]
        more_comment_data = get_comment_data_from_uri( d, o )

        comment_data["comments"] += more_comment_data["comments"]
        comment_data["cursor"] = [ comment_data["cursor"] , more_comment_data["cursor"] ].flatten
    end
   
    comment_data[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")

    o[:cursor] = 0

    # intergrate "human" behavior (https://www.youtube.com/watch?v=p0mRIhK9seg&t=131s)
    random_number = rand(1..5)
    sleep random_number 

    return comment_data 

end

def get_comment_data( d,o )
            
    item_id = d["aweme_id"]
    comment_id= d["comment_id"]
    

    if o[:prefix].nil?
        o[:prefixid] = "#{o[:ingest_data][:prefixid]}_#{ o[:ingest_data][:provider][:@id].downcase }_#{ o[:ingest_data][:dataset][:@id].downcase }"
    end

    #### TODO
    # What if the file_path contains new or processed ????

    if comment_id.nil?
        file = File.join( o[:file_path], "comments", "#{o[:prefixid]}_#{item_id}.json" )
    else
        file = File.join( o[:file_path], "comments", "#{o[:prefixid]}_#{item_id}_#{comment_id}.json" )
    end
    
    comment_data = get_comment_data_from_file( file )

    unless comment_data.nil?
        comment_data = comment_data["data"]
    else
        comment_data = get_comment_data_from_uri( d, o )
        
        icandid_output = IcandidCollector::Output.new( data: { data: comment_data}, icandid_config: o[:config])
        icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})

    end

    comment_data["comments"].map! do |comment|
        comment_ids = {
            "aweme_id"   => comment["aweme_id"] ,
            "comment_id" => comment["cid"] 
        }

        unless comment["reply_comment_total"].nil?
            if comment["reply_comment_total"] > 0

                recursive_comment_data = get_comment_data(comment_ids, o )
                comment["comments"] = recursive_comment_data["comments"]
            end
        end
        comment["download_time"] = comment_data["download_time"]
        comment
    end
    return comment_data

end

@rule_set_name = "RULE_SET_v0_1"

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
        },
        expand_record: { "$.data.videos" => lambda { |d,o|
            @rule_set_name = eval(@rule_set_name)
            out = DataCollector::Output.new
            rules_ng.run( @rule_set_name[:rs_expand_record], d, out, o)
        }}
    },
    rs_expand_record: {
        user: { "$.username" => lambda { |d,o|

            o[:use_screen_scraping]  =  o[:use_screen_scraping] || true

            if o[:prefix].nil?
                o[:prefixid] = "#{o[:ingest_data][:prefixid]}_#{ o[:ingest_data][:provider][:@id].downcase }_#{ o[:ingest_data][:dataset][:@id].downcase }"
            end

            #### TODO
            # What if the file_path contains new or processed ????
            
            file = File.join( o[:file_path], "users", "#{o[:prefixid]}_#{d}.json" )

            icandid_input = IcandidCollector::Input.new( :icandid_config => {} )
            data = icandid_input.collect_data_from_uri(url: "file://#{file}",  options: {} )
            
            #pp "TEST USERDATA 1"
            unless data.nil?
                user_data = data["data"]
                return user_data
            end
            
            #pp "TEST USERDATA 2"
            unless o[:use_screen_scraping] # don't use screenscrape for users! It uses too much requests (API has a limit of 1000 requests per day)
                url = "https://open.tiktokapis.com/v2/research/user/info/?fields=display_name,bio_description,avatar_url,is_verified,follower_count,following_count,likes_count,video_count"

                pp " Don't use the API to collect userdata. API has a limit of 1000 requests per day !!!"
                raise " Don't use the API to collect userdata. API has a limit of 1000 requests per day !!!"
                exit
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
            
            if o[:use_screen_scraping]
                http = HTTP
                url = "https://www.tiktok.com/@#{d}"
                http_response = http.follow.get(url, {})
             #   pp "TEST USERDATA 3"
             #   pp "TEST #{url}"
                data = http_response.body.to_s
                raw_data = Nokogiri::HTML(data)
                
             #   pp "TEST USERDATA 4"
                test_data =  raw_data.xpath("/html/body/script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']")
             #    pp "TEST USERDATA 5"
                 test_text = test_data.text
             #    pp "TEST USERDATA 6"
                jdata = JSON.parse( raw_data.xpath("/html/body/script[@id='__UNIVERSAL_DATA_FOR_REHYDRATION__']").text )
                
                # pp jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]["user"].keys
                # pp "shareMeta => #{jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["shareMeta"]}"
                
             #   pp "TEST USERDATA 7"
                if jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"].has_key?("userInfo")
                    user_data = jdata["__DEFAULT_SCOPE__"]["webapp.user-detail"]["userInfo"]
                else
                    user_data = {
                        "error" => {
                            "code" => "user not found for id: #{d}"
                        },
                        "user" => {
                            "uniqueId" => d,
                            "description" => "Couldn't find this account [#{d}]",
                            "name" => "unknown for #{d}",
                        },
                        "stats" => {}
                    }
                end
                user_data[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                
                icandid_output = IcandidCollector::Output.new( data: { data: user_data}, icandid_config: o[:config])
                icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})

            end

            user_data
        }},
        comment:  { "$" => lambda { |d,o|
            comment_data = nil
            if d["comment_count"] > 0

                message_id = d["id"]
                username = d["username"]

                # d["id"] = 7471305239946300674
                # d["id"] = 7480130122155150614
                # d["id"] = 7480153394636737814

                comment_ids = {
                    "aweme_id" => d["id"].to_s,
                    "comment_id" => nil
                }
                out = DataCollector::Output.new
                rules_ng.run(@rule_set_name[:rs_expand_comment], comment_ids, out, o)
                comment_data = out[:comment]
            end
            comment_data
        }}
    },
    rs_expand_comment: {
        comment:  { "$" => lambda { |d,o|

            if d["aweme_id"].nil?
                raise "Error expanding comment: aweme_id missing !"
            end
            comment_data = get_comment_data(d, o)
            comment_data
            
        }}
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
            
            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
            end


            o[:use_screen_scraping] = o[:use_screen_scraping] || true

            unless o[:use_screen_scraping]
                puts "RULES RULES RULES records o[:tiktokusers] #{o[:tiktokusers].keys.size}"
                @tiktokusers = o[:tiktokusers]
            end

            o[:file_path] = File.dirname(o[:file])

            out = DataCollector::Output.new
            rules_ng.run( @rule_set_name[:rs_record], d, out, o )

            if out[:record].nil?
                pp d.keys
                pp "MAYDAY_MAYDAY"
                pp out
            end
           
            out[:record]
            } ] 
        },
        options: { "$.data." => [ lambda { |d,o|
                o[:use_screen_scraping] = o[:use_screen_scraping] || true
                unless o[:use_screen_scraping]
                    o[:tiktokusers] = @tiktokusers
                end
                o
            }] 
        }
    },
    rs_record: {
        record: { "$.videos" => lambda { |d,o| 

            expand_out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_expand_record], d, expand_out, o)

            d["userdata"] = expand_out["user"] unless
            d["comment"] = expand_out["comment"] unless expand_out["comment"].nil? || expand_out["comment"].empty?

            #pp "RULES RULES RULESrs_expand_record"
            #pp out["user"]
            #pp "-------------------------------------------------"
            #pp "RULES RULES RULESrs_expand_record"

            rdata = {}

            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_id], d, out, o)
            o[:id] = out[:id].first


            if o[:ingest_data]["metaLanguage"] == "und" || o[:ingest_data]["metaLanguage"].nil?
                o[:detectedLanguage] = @icandid_utils.languageDetection("#{d["voice_to_text"]} #{d["video_description"]}" , {language_detection_url:  o[:config][:language_detection_url]})
            end

            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear
            o[:contextLanguage] = rdata["@context"]["@language"]

            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.data)
            rdata[:creator] = rdata[:author] = rdata[:sender] 

            if rdata[:inLanguage].nil?
                langcode = o[:contextLanguage] 
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
        sender: {'$.userdata' => lambda { |d,o| 

            user_id = d["user"]["uniqueId"]
            user_data = d["user"]

            user = { 
                :@id         => user_id,
                :identifier => {
                        :@type  => "PropertyValue",
                        :name   => "verified",
                        :@id    => "tiktok_verified_true",
                    },
                :@type       => "Person",
                :sameAs      => "https://www.tiktok.com/@#{user_id}",
                :memberOf    => {
                    :@type => "OrganizationRole",
                    :roleName => ["user"],
                    :@id => "iCANDID_tiktok_PERSON_ORGANIZATION_ROLE_#{user_id.upcase}",
                    :memberOf => {
                        :@type => "Organization",
                        :name => "TikTok",
                        :@id => "iCANDID_ORGANIZATION_TIKTOK"
                    }
                }
            } 

            #user[:description] = "Couldn't find this account [#{user_id}]"
            #user[:name] = "unknown for #{user_id}"
            
            user[:identifier][:value] = d["user"]["verified"] 
            user[:description] = d["user"]["signature"] 
            user[:name] = d["user"]["nickname"]
            user[:logo] = d["user"]["avatarThumb"]

            if d["user"]["verified"]
                user[:memberOf][:roleName] << "verified user"
            end

            user[:interactionStatistic] = []
            unless d["stats"]["heartCount"].nil?
                user[:interactionStatistic] << { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/LikeAction",
                    :userInteractionCount => d["stats"]["heartCount"],
                    :endTime              => d["download_time"]
                }
            end
            unless d["stats"]["videoCount"].nil?
                user[:interactionStatistic] <<  { 
                    :@type                => "InteractionCounter",
                        :interactionType      => "https://schema.org/CommunicateAction",
                    :userInteractionCount => d["stats"]["videoCount"],
                    :endTime              => d["download_time"]
                }
            end
            unless  d["stats"]["followerCount"].nil?
                user[:interactionStatistic] <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/FollowAction",
                    :userInteractionCount =>  d["stats"]["followerCount"],
                    :endTime              => d["download_time"]
                }
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
            url = "https://www.tiktok.com/@#{d["username"]}/video/#{d["id"]}"
            
            if o[:download_video]
                out = DataCollector::Output.new
                rules_ng.run(RULE_SET_VIDEO_DOWNLOAD_v0_1[:rs_records], d, out, o)
            end

            url
            
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
            unless d.nil? || d.empty?
                c = ISO3166::Country.new(d)
                
                if  c.nil?
                    country = d
                else
                    country = c.translations[I18n.locale.to_s] || c.name
                end

                {   
                    :name          => country,
                    :@type         => "Place",
                    :alternateName => d.downcase
                }
            end
        }},
        inLanguage: { "$" =>  lambda { |d,o| 
            unless Iso639[d].nil? || Iso639[d].alpha2.to_s.empty?
                langcode = d
            else
                langcode = o[:contextLanguage] 
            end
            {
                :@type         => "Language",
                :@id           => Iso639[langcode].alpha2,
                :name          => Iso639[langcode].name,
                :alternateName => Iso639[langcode].alpha2,
            }
        }},
        comment: { "$.comment" => lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_comments], d, out, o)
            out[:comments]
        }}
    },
    rs_comments: {
        comments: { "$.comments" => lambda { |d,o|
            o[:downloadtime] = d["download_time"]
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_LANGUAGE_HELPERS[:rs_detect_language_script], d["comment_language"].downcase, out, o)
            o[:comment_language] = "#{d["comment_language"].downcase}-#{out[:detect_language_script][0]}"
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_comment], d, out, o)
            out["@type"] = "Comment"
            out.raw
        }}
    },
    rs_comment:{
        "@id": "$.cid",
        "@type": "Comment",
        name: {"$"=> lambda { |d,o|

            if  d["share_info"]["desc"].nil? || d["share_info"]["desc"].empty?
                {
                    :@value => "#{d["user"]["nickname"]}’s comment: #{d["text"]}",
                    :@language => o[:comment_language]
                }
            else
                {
                    :@value => d["share_info"]["desc"],
                    :@language => o[:comment_language]
                }
            end
        }},
        text: {"$.text"=> lambda { |d,o|
            {
                :@value => d,
                :@language => o[:comment_language]
            }
        }},
        create_time: { "$.create_time" => lambda { |d,o|
            Time.at(d).strftime("%Y-%m-%d")
        }},
        interactionStatistic: { "$.digg_count" => lambda { |d,o|
            unless d == "0"
                { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/LikeAction",
                    :userInteractionCount => d,
                    :endTime              => o[:downloadtime]
                }
            end
        }},
        author: { "$.user" => lambda { |d,o|
            {
                :@id           => d["uid"],
                :name          => d["nickname"],
                :alternateName => d["unique_id"]
            }
        }},
        # sameAs: "$.share_info.url",
        inLanguage: { "$.comment_language" =>  lambda { |d,o| 
            unless Iso639[d].nil? || Iso639[d].alpha2.to_s.empty?
                langcode = d
            else
                langcode = o[:contextLanguage] 
            end
            {
                :@type         => "Language",
                :@id           => Iso639[langcode].alpha2,
                :name          => Iso639[langcode].name,
                :alternateName => Iso639[langcode].alpha2,
            }
        }},        
        comment: { "$.comments" => lambda { |d,o|
            o[:downloadtime] = d["download_time"]
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_comment], d, out, o)
            return out.raw
        }}
    }
}
