#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

@rule_set_name = "RULE_SET_v0_1"

RULE_SET_v0_1 = {
    version: "0.1",
    rs_filename:{
        filename: { "$.items" => lambda { |d,o| 
                unless d["id"].empty?
                    "youtube_#{d["id"]}_.json"
                end
            }
        }
    },  
    rs_filename_audio:{
        filename_audio: { "$.items" => lambda { |d,o| 
                unless d["id"].empty?
                    "youtube_#{d["id"]}_.flac"
                end
            }
        }
    },
    rs_url:{
        url: { "$.id" => lambda { |d,o| 
                unless d.empty?
                    "https://youtu.be/#{d}"
                end
            }
        }
    },    
    rs_raw_data:{
        data: { "$.items" => lambda { |d,o| 
               d
            }
        },
        error: { "$.error" => lambda { |d,o| 
            d
            }
        }
    },
    rs_id:{
        id:  {'$.items.*.id' =>  lambda { |d,o| 
            d
        }}
    },    
    rs_channelId:{
        channelId:  {'$.items.*.snippet.channelId' =>  lambda { |d,o| 
            d
        }}
    },    
    rs_categoryId:{
        categoryId:  {'$.items.*.snippet.categoryId' =>  lambda { |d,o| 
            d
        }}
    },
    rs_videoid:{
        id: {'$.id' => lambda { |d,o|
            d
        }}
    },
    rs_records: {
        records: { "$.data.*" => [ lambda { |d,o| 

            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
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
        }
    },
    rs_record: {
        record: { "$.items[0]" => lambda { |d,o| 


            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_videoid], d, out, o)

            o[:id] = out[:id].first


            rdata = {}
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear
            o[:contextLanguage] = rdata["@context"]["@language"]

            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.data)

            if rdata[:inLanguage].nil?
                langcode = o[:contextLanguage] 
                rdata[:inLanguage] =  {
                    :@type         => "Language",
                    :@id           => Iso639[langcode].alpha2,
                    :name          => Iso639[langcode].name,
                    :alternateName => Iso639[langcode].alpha2
                }
            end
            rdata.compact
        } }
    },
    rs_creator_data: {
        creator: {'$' => lambda { |d,o|
            s = d["snippet"]
            user = { 
                :@id         => "ICANDID_youtube_PERSON_#{d["id"]}",
                :@type       => "Person", 
                :name => s["title"],
                :sameAs      => "https://youtube.com/#{s["customUrl"]}",
                :memberOf    => {
                    :@type => "OrganizationRole",
                    :roleName => ["user"],
                    :@id => "iCANDID_youtube_PERSON_ORGANIZATION_ROLE_#{d["id"]}",
                    :memberOf => {
                        :@type => "Organization",
                        :name => "YouTube",
                        :@id => "iCANDID_ORGANIZATION_YOUTUBE"
                    }
                }
            }
            unless s["description"].nil? || s["description"].empty?
                user[:description] = s["description"]
            end
            user[:interactionStatistic] = [] 
            unless d["statistics"]["viewCount"].nil?
                user[:interactionStatistic] << { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/ViewAction",
                    :userInteractionCount => d["statistics"]["viewCount"],
                }
            end
            unless d["statistics"]["subscriberCount"].nil?
                user[:interactionStatistic] << { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/SubscribeAction",
                    :userInteractionCount => d["statistics"]["subscriberCount"],
                }
            end
            unless d["statistics"]["videoCount"].nil?
                user[:interactionStatistic] << { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/CreateAction", 
                    :userInteractionCount => d["statistics"]["videoCount"],
                }
            end
            user
        }}
    },
    rs_language_data: {
        :inLanguage => { '$' => lambda { |d,o|
            langcode = d
            rdata =  {
                :@type         => "Language",
                :@id           => Iso639[langcode].alpha2,
                :name          => Iso639[langcode].name,
                :alternateName => Iso639[langcode].alpha2
            }
            rdata
        }}
    },
    rs_record_data: {
        name: '$.snippet.title',
        datePublished: {'$.snippet.publishedAt' =>  lambda { |d,o| 
            Time.parse(d).strftime("%Y-%m-%d")
        }},
        duration: '$.contentDetails.duration',
        creator: {"$.channel" => lambda { |d,o| 
            out = DataCollector::Output.new
            rdata = {}
            rules_ng.run(@rule_set_name[:rs_creator_data], d, out, o)
            rdata.merge!(out[:creator].to_h)
            rdata
        }},
        interactionStatistic:  {'$.statistics' =>  lambda { |d,o| 
            rdata = []
            unless d["viewCount"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/ViewAction",
                    :userInteractionCount => d["viewCount"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end
            unless d["likeCount"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/LikeAction",
                    :userInteractionCount => d["likeCount"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end
            unless d["commentCount"].nil?
                rdata <<  { 
                    :@type                => "InteractionCounter",
                    :interactionType      => "https://schema.org/CommentAction",
                    :userInteractionCount => d["commentCount"],
                    :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                }
            end     
            rdata 
        } },
        keywords: '$.snippet.tags',
        thumbnailUrl: {'$.snippet.thumbnails' => lambda { |d,o|
            url = nil
            max = 0
            d.each do |key, value|
                if (value["width"] > max)
                    url = value["url"]
                    max = value["width"]
                end
            end
            url
        }},
        inLanguage: { '$.snippet.defaultAudioLanguage' => lambda { |d, o|
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_language_data], d, out, o)
            out[:inLanguage]
        }},
        sameAs: {'$' => lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_url], d, out, o)
            out[:url]
        }},
        comment: {'$' => lambda { |d,o|
            out = DataCollector::Output.new
            rules_ng.run(@rule_set_name[:rs_comments], d, out, o)
            out[:comments]
        }}   
    },
    rs_comments: {
        comments: { '$.comments' => lambda { |d,o|
            rdata = {
                "@id": "ICANDID_youtube_COMMENT_#{d["id"]}",
                "@type": "Comment",
            }   
            out = DataCollector::Output.new
            if d["snippet"]["topLevelComment"].nil?
                rules_ng.run(@rule_set_name[:rs_comment], d["snippet"], out, o)
            else
                rules_ng.run(@rule_set_name[:rs_comment], d["snippet"]["topLevelComment"]["snippet"], out, o)
            end
            rdata.merge!(out[:comment])
            
            unless d["snippet"]["totalReplyCount"].nil?
                unless d["snippet"]["totalReplyCount"] == "0"
                    rdata["interactionStatistic"].append ({ 
                        :@type                => "InteractionCounter",
                        :interactionType      => "https://schema.org/ReplyAction",
                        :userInteractionCount => d["snippet"]["totalReplyCount"],
                        :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                    })
                end
            end
            unless d["replies"].nil?
                out = DataCollector::Output.new
                rules_ng.run(@rule_set_name[:rs_comments], d["replies"], out, o)
                rdata["comment"] = out[:comments]
            end
            rdata
            }
        }
    },
    rs_comment: {
        comment: {'$' => lambda { |d, o|
            out = {
                text: d["textOriginal"],
                create_time: Time.parse(d["publishedAt"]).strftime("%Y-%m-%d"),
                author: {
                    :@id           => "ICANDID_youtube_PERSON_#{d["authorChannelId"]["value"]}",
                    :@type         => "Person",
                    :name          => d["authorDisplayName"],
                    :alternateName => d["authorChannelId"]["value"],
                    :sameAs        => d["authorChannelUrl"] 
                },
                interactionStatistic: []
            }

            unless d["likeCount"].nil?
                unless d["likeCount"] == "0"
                    out[:interactionStatistic].append( { 
                        :@type                => "InteractionCounter",
                        :interactionType      => "https://schema.org/LikeAction",
                        :userInteractionCount => d["likeCount"],
                        :endTime              => Time.parse(o[:file_created_at]).strftime("%Y-%m-%dT%H:%M:%SZ")
                    })
                end
            end
            out
        }}
    }
}
