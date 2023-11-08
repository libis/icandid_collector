#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v2_2 = {
    version: "2.2",
    rs_records: {
        records: { "$.data" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            #out.clear          
            #haal data op
            rules_ng.run(RULE_SET_v2_2[:rs_data], d, out, o)
            data = out[:data] 
            #out.clear        
            data        
        } ] }
    },
    rs_data: {
        data: { "@" => lambda {|d,o|
# Tweet object (d)
=begin
created_at             => publication_date
id                     => identifier , sameAs, @id, ...
text                   => name, articleBody 
attachments        => associatedMedia
attachments.media_keys => associatedMedia
author_id              => author, sender
context_annotations    =>  TODO : _named_entitie ????????
conversation_id        => isPartOf (conversation with messages?) ?????
entities           => keywords, mentions
entities.annotations   
entities.cashtags 
entities.hashtags      => keywords
entities.mentions      => mentions
entities.urls          =>  TODO : Media / messageAttachment ???????? 
geo                => contentLocation
geo.coordinates        => contentLocation
geo.place_id           => contentLocation
in_reply_to_user_id    => recipient
lang                   => inLanguage
# non_public_metrics
# organic_metrics
possiby_sensitive      
# promoted_metrics
# public_metrics
referenced_tweets
referenced_tweets(replied_to) => identifier(PropertyValue) "replied_to_tweet_id"
referenced_tweets(quoted)     => identifier(PropertyValue) "quoted_tweet_id" 
referenced_tweets(retweeted)  => identifier(PropertyValue) "retweeted_tweet_id" 
reply_settings
# source
# withheld
=end 
            # https://developer.twitter.com/en/docs/twitter-api/premium/data-dictionary/object-model/tweet
            rdata = {
                :datePublished => d["created_at"],
                :identifier    => [ {
                    :@type => "PropertyValue", 
                    :@id   => "tweet_id_#{d["id"]}",
                    :name  => "tweet_id", 
                    :value => d["id"]} 
                ],
                :@id           => "#{o[:prefixid]}_#{d["id"]}",
                :sameAs        => "https://twitter.com/temp/status/#{d["id"]}",
                :name          => d["text"],
                :text          => d["text"],
                :publisher     => {
                    :@type => "Organization",
                    :@id   => "iCANDID_ORGANIZATION_TWITTER",
                    :name  => "Twitter"
                }
            }
            user = { :@id =>  "#{o[:prefixid]}_PERSON_#{d["author_id"]}" }
            
            unless o[:users].empty?
                #user = o[:users].select { |user| user[:@id] == "#{o[:prefixid]}_PERSON_#{d["author_id"]}"  }.first
                user = o[:users]["#{o[:prefixid]}_PERSON_#{d["author_id"]}"]
            end
            rdata[:author] = user
            rdata[:sender] = user

            unless d["in_reply_to_user_id"].nil?
                user = { :@id =>  "#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}" }
                unless o[:users].empty?
                    #user = o[:users].select { |user| user[:@id] == "#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}" }.first
                    user = o[:users]["#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}"]
                end
                rdata[:recipient ] = user
            end

            unless d["geo"].nil?
                unless d["geo"]["place_id"].nil?
                    #location = o[:places].select{ |place| place[:@id] == "#{o[:prefixid]}_PLACE_#{d["geo"]["place_id"]}" }.first 
                    location = o[:places]["#{o[:prefixid]}_PLACE_#{d["geo"]["place_id"]}"] || {}
                    unless d["geo"]["coordinates"].nil? || location.empty?
                        geoCoordinates = {
                                :@type     => "GeoCoordinates",
                                :@id       => "#{o[:prefixid]}_PLACE_COORDINATES_#{d["geo"]["place_id"]}",
                                :latitude  => d["geo"]["coordinates"]["coordinates"][0],
                                :longitude => d["geo"]["coordinates"]["coordinates"][1]
                        }
                        if location[:geo].nil?
                            location[:geo] = geoCoordinates
                        else
                            location[:geo] = [location[:geo]] if !location[:geo].is_a?(Array)
                            location[:geo] << geoCoordinates
                        end
                        rdata[:contentLocation] = location
                    end
                end
            end

            o[:twitter_record_id] = d["id"]

            out = DataCollector::Output.new
            #out.clear 

            rules_ng.run(RULE_SET_v2_2[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h)
            out.clear

            #"lang": "de"
            rules_ng.run(RULE_SET_v2_2[:rs_in_language], d, out, o)
            
            #"entities": { "hashtags": [] 
            rules_ng.run(RULE_SET_v2_2[:rs_keywords], d, out, o)
            
            #"entities": { "user_mentions": [] 
            rules_ng.run(RULE_SET_v2_2[:rs_mentions], d, out, o)            
            
            rules_ng.run(RULE_SET_v2_2[:rs_associated_media], d, out, o)

            rdata.merge!(out.to_h)
            out.clear

            rules_ng.run(RULE_SET_v2_2[:rs_conversation], d, out, o)
            unless out[:conversation].to_h.empty?
                rdata[:identifier] << out[:conversation].to_h[:identifier] 
                rdata[:isPartOf] = out[:conversation].to_h[:isPartOf] 
            end

            unless d["referenced_tweets"].nil?
                d["referenced_tweets"].each { |referenced_tweet|
                    if rdata[:identifier].nil?
                        rdata[:identifier] = []
                    end
                    case referenced_tweet["type"]
                    when "replied_to"
                        rdata[:identifier] << {
                            :@type => "PropertyValue", 
                            :@id   => "replied_to_tweet_id_#{referenced_tweet["id"]}",
                            :name  => "replied_to_tweet_id",
                            :value => referenced_tweet["id"],
                            :url   => "/#/record/#{o[:prefixid]}_#{referenced_tweet["id"]}"
                          }
                    when "quoted"
                        rdata[:identifier] << {
                            :@type => "PropertyValue", 
                            :@id   => "quoted_tweet_id_#{referenced_tweet["id"]}",
                            :name  => "quoted_tweet_id",
                            :value => referenced_tweet["id"],
                            :url   => "/#/record/#{o[:prefixid]}_#{referenced_tweet["id"]}"
                          }
                    when "retweeted"
                        rdata[:identifier] << {
                            :@type => "PropertyValue", 
                            :@id   => "retweeted_tweet_id_#{referenced_tweet["id"]}",
                            :name  => "retweeted_tweet_id",
                            :value => referenced_tweet["id"],
                            :url   => "/#/record/#{o[:prefixid]}_#{referenced_tweet["id"]}"
                          }
                        #ref_tweet = o[:tweets].select { |tweet| tweet[:@id] == "#{o[:prefixid]}_#{ referenced_tweet["id"] }" }.first
                        ref_tweet = o[:tweets]["#{o[:prefixid]}_#{ referenced_tweet["id"] }"]
                        unless ref_tweet.nil?
                            rdata[:text] = ref_tweet[:text]
                            unless ref_tweet[:keywords].nil?
                                if rdata[:keywords].nil?
                                    rdata[:keywords] = ref_tweet[:keywords]
                                else
                                    rdata[:keywords] = [rdata[:keywords] ] if !rdata[:keywords].is_a?(Array)
                                    rdata[:keywords] << ref_tweet[:keywords]
                                end
                            end
                            unless ref_tweet[:mentions].nil?
                                if rdata[:mentions].nil?
                                    rdata[:mentions] = ref_tweet[:mentions]
                                else
                                    rdata[:mentions] = [rdata[:mentions] ] if !rdata[:mentions].is_a?(Array)
                                    ref_tweet[:mentions] = [ref_tweet[:mentions] ] if !ref_tweet[:mentions].is_a?(Array)
                                    ref_tweet[:mentions].each { |m|
                                        unless rdata[:mentions].map { |m| m[:@id] }.include?(m[:@id])
                                            rdata[:mentions] << m
                                        end
                                    }
                                end
                            end
                            # The user is not the creator of the message, is it only th sender ???                            
                            ref_tweet_username = rdata[:name].scan(/^RT @[^:]*/)[0]
                            unless ref_tweet_username.nil?
                                ref_tweet_username = ref_tweet_username[4..-1].to_s
                                user = o[:users].values.select { |user| user[:alternateName] == ref_tweet_username }.first
                                rdata[:author] = user
                            end
                        end

                    end
                }
            end            

            rdata
        }
      }
    },
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            {
                :@id            => "#{o[:prefixid]}_#{d["id"]}",
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    :@id      => "#{INGEST_CONF[:prefixid]}_#{INGEST_CONF[:provider][:@id] }_#{INGEST_CONF[:dataset][:@id]}",
                    :license  => INGEST_CONF[:license],
                    :name     => INGEST_CONF[:genericRecordDesc],
                    :provider => INGEST_CONF[:provider],
                    :isPartOf => {
                        :@id   => INGEST_CONF[:dataset][:@id],
                        :@type => "Dataset",
                        :name  => INGEST_CONF[:dataset][:name]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{ INGEST_CONF[:metaLanguage]}-#{ INGEST_CONF[:unicode_script]}" }]    
            }
        }}
    },
    rs_associated_media: {
        associatedMedia: { "@.attachments.media_keys" => lambda { |d,o| 
            #o[:media].select { |media| media[:@id] == "#{o[:prefixid]}_MEDIA_#{ d }" }.first
            o[:media]["#{o[:prefixid]}_MEDIA_#{ d }"]
        }}
    },
    rs_in_language: {
        inLanguage: { "$.lang" =>  lambda { |d,o| 
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
    },
    rs_keywords: {
        keywords: {
            "$.entities.hashtags..tag" => 'text'
        }
    },
    rs_mentions: {
        mentions: {
            "$.entities.mentions..username" => lambda { |d,o| 
                o[:users].values.select { |user| user[:alternateName] == d }.first
            }
        }
    },
    rs_conversation: {
        conversation: { "@" => lambda { |d,o|  
            unless d["conversation_id"].nil?
                unless d["conversation_id"] == d["id"]
                    #puts  "====================> conversation_id #{ d["conversation_id"] }"
                    conversation = { 
                        :identifier => {
                            :@type => "PropertyValue", 
                            :@id   => "conversation_tweet_id_#{d["conversation_id"]}",
                            :name  => "conversation_tweet_id",
                            :value => d["conversation_id"] 
                        },
                        :isPartOf => {
                            :@type => "Conversation",
                            :@id   => "#{o[:prefixid]}_CONVERSATION_#{d["conversation_id"]}",
                            :name  => "Twitter conversation #{d["conversation_id"]}"
                        }
                    }
                    unless o[:tweets].nil?
                        #tweet = o[:tweets].select { |tweet| tweet[:@id] == "#{o[:prefixid]}_#{ d["conversation_id"] }" }.first
                        #puts  o[:tweets]
                        #puts  o[:tweets].keys
                        tweet = o[:tweets]["#{o[:prefixid]}_#{d["conversation_id"]}"]
                        unless tweet.nil?
                            conversation[:isPartOf][:name] = tweet[:text]
                        end
                    end
                end
            end
            conversation
        }}
    },

    rs_users: {
        users:  { "@.includes.users" => lambda { |d,o|  
# User object (d)
=begin
id                 => @id 
name               => name
username           => alternateName
created_at         => memberOf.startDate
description        => description
entities
location           => address
pinned_tweet_id
profile_image_url
protected
public_metrics
url
verified           => memberOf.roleName, identifier { PropertyValue,verified } 
withheld
=end                  
            unless d["url"].nil?
                d["url"] = "https://#{d["url"]}" unless d["url"].start_with?("http://","https://")
            end
            d["name"] = d["username"] if d["name"].empty?
          
            u = {
                :@type         => "Person",
                :@id           => "#{o[:prefixid]}_PERSON_#{d["id"]}",
                :name          => d["name"],
                :alternateName => d["username"], 
                :sameAs        => "https://twitter.com/#{d["username"]}",
                :description   => d["description"],
                :url           => d["url"],
                :address       => d["location"]
            }
            unless d["created_at"].nil?
                u[:memberOf] = {
                    :@type    => "OrganizationRole",
                    :@id      => "#{o[:prefixid]}_PERSON_ORGANIZATION_ROLE_#{d["id"]}",
                    :memberOf => {
                        :@type => "Organization",
                        :@id   => 'iCANDID_ORGANIZATION_TWITTER',
                        :name  => "Twitter"
                    },
                    :startDate => d["created_at"],
                    :roleName  => ["user"]
                }
                if d["verified"] == true
                    u[:memberOf][:roleName] << "verified user"
                end
            end 

            if d["verified"] == true
                u[:identifier] = [
                    {
                      :@type => "PropertyValue",
                      :@id   => "twitter_verified_true",
                      :name  => "verified",
                      :value => d["verified"]
                }]
            end
            u
        }}
    },
    rs_media: {
        media:  { "@.includes.media" => lambda { |d,o|  
# Media object (d)
=begin
media_key         => @id
type              => ImageObject ; VideoObject ; AudioObject
duration_ms       => duration
height            => height
non_public_metrics
organic_metrics
preview_image_url => thumbnailUrl
promoted_metrics
public_metrics
width             => width 
=end                  
            m = {
                :@type         => "MediaObject",
                :@id           => "#{o[:prefixid]}_MEDIA_#{d["media_key"]}",
                :width         => d["width"],
                :height        => d["height"],
                :duration      => d["duration_ms"],
                :thumbnailUrl  => d["preview_image_url"],
                :url           => d["url"]
            }
            case d["type "]
            when "photo", "animated_gif"
                m[:@type] = ImageObject 
            when "video"
                m[:@type] = VideoObject
            end
            m
        }}
    },
    rs_places: { 
# place Object (d)
=begin
full_name        => name
id               => @id
contained_within =>
country
country_code     => geo(GeoShape).addressCountry
geo              => geo(GeoShape).box
name             => geo(GeoShape).address
place_type
=end  
        places: { "$.includes.places" =>  lambda {|d, o|
            r = {
                :@type         => "Place",
                :@id           => "#{o[:prefixid]}_PLACE_#{d["id"]}",
                :name          => d["name"],
            }
            if d["geo"]["type"] == "Feature"
                r[:geo] = {
                    :@type => "GeoShape",
                    :@id   => "#{o[:prefixid]}_PLACE_SHAPE_#{d["id"]}",
                    :box   => "#{d["geo"]["bbox"][0]},#{d["geo"]["bbox"][1]} #{d["geo"]["bbox"][2]},#{d["geo"]["bbox"][3]}",
                    :address        => "#{d["full_name"]} (#{d["place_type"]})",
                    :addressCountry => d["country_code"]
                }
            else
                r[:geo] = {
                    :@type => "GeoShape",
                    :@id   => "#{o[:prefixid]}_PLACE_SHAPE_#{d["id"]}",
                    :address        => "#{d["full_name"]} (#{d["place_type"]})",
                    :addressCountry => d["country_code"]
                }
            end
            r
        }}
    },    
=begin      
# geo object in schema.org
place.geo : {
    type(GeoCoordinates)
    address
    addressCountry
    elevation
    latitude
    longitude
    postalCode
},
place.geo : {
    type(GeoShape)
    address
    addressCountry
    box
    circle
    elevation
    line
    polygon
    postalCode
},
=end
    rs_tweets: { 
    # tweet Object (d)
        tweets: { "$.includes.tweets" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            # out.clear          
            #haal data op
            rules_ng.run(RULE_SET_v2_2[:rs_data], d, out, o)
            data = out[:data] 
            out.clear        
            data        
        }]}
    }
}
