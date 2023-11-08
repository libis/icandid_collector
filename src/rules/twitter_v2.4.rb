#encoding: UTF-8
require 'data_collector'
require "iso639"

RULE_SET_v2_4 = {
    version: "2.4",
    rs_records: {
        records: { "@" => lambda { |d,o|  


            o[:media] = {}
            media = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_media], d, media, o)
            unless media[:media].nil?
                o[:media] = media[:media] 
                o[:media] = [ o[:media] ] unless o[:media].kind_of?(Array)
                o[:media] = o[:media].map{ |e| [e[:@id], e] }.to_h
            end

            o[:places] = {}
            places = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_places], d, places, o)
            unless places[:places].nil?
                o[:places] = places[:places] 
                o[:places] = [ o[:places] ] unless o[:places].kind_of?(Array)
                o[:places] = o[:places].map{ |e| [e[:@id], e] }.to_h
            end

            o[:users] = {}
            users = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_users], d, users, o)
            unless users[:users].nil?
                o[:users] = users[:users] 
                o[:users] = [ o[:users] ] unless o[:users].kind_of?(Array)
                o[:users] = o[:users].map{ |e| [e[:@id], e] }.to_h
            end


            o[:includes_tweets] = {}
            includes_tweets = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_includes_tweets], d, includes_tweets, o)
            unless includes_tweets[:includes_tweets].nil?
                o[:includes_tweets] = includes_tweets[:includes_tweets] 
                o[:includes_tweets] = [ o[:includes_tweets] ] unless o[:includes_tweets].kind_of?(Array)
                o[:includes_tweets] = o[:includes_tweets].map{ |e| [e[:@id], e] }.to_h
            end
            
            records = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_record], d, records, o)
            records[:records]

        } }
    },
    rs_record: {
        records: { "$.data" => [ lambda { |d,o|  
            record = DataCollector::Output.new
            #out.clear          
            #haal data op
            rules_ng.run(RULE_SET_v2_4[:rs_data_tweets], d, record, o)
            record[:data] 
        } ] }
    },

    rs_data_tweets: {
        data: { "@" => [ lambda { |d,o|  

# Tweet object (d)
# =begin
# created_at             => publication_date
# id                     => identifier , sameAs, @id, ...
# text                   => name, articleBody 
# attachments        => associatedMedia
# attachments.media_keys => associatedMedia
# author_id              => author, sender, creator
# context_annotations    =>  TODO : _named_entitie ????????
# conversation_id        => isPartOf (conversation with messages?) ?????
# entities           => keywords, mentions
# entities.annotations   
# entities.cashtags 
# entities.hashtags      => keywords
# entities.mentions      => mentions
# entities.urls          =>  TODO : Media / messageAttachment ???????? 
# geo                => contentLocation
# geo.coordinates        => contentLocation
# geo.place_id           => contentLocation
# in_reply_to_user_id    => recipient
# lang                   => inLanguage
# # non_public_metrics
# # organic_metrics
# possiby_sensitive      
# # promoted_metrics
# # public_metrics
# referenced_tweets
# referenced_tweets(replied_to) => identifier(PropertyValue) "replied_to_tweet_id"
# referenced_tweets(quoted)     => identifier(PropertyValue) "quoted_tweet_id" 
# referenced_tweets(retweeted)  => identifier(PropertyValue) "retweeted_tweet_id" 
# reply_settings
# # source
# # withheld
# =end 
# https://developer.twitter.com/en/docs/twitter-api/premium/data-dictionary/object-model/tweet

            tweet = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_tweets], d, tweet, o)
            rdata = tweet[:tweets].to_h
            # out.clear

            # Expand User
            user_id = rdata[:creator][:@id]
            unless o[:users].empty?
                user = o[:users][ user_id ]
                unless user.nil? 
                    rdata[:author] = user
                    rdata[:creator] = user
                    rdata[:sender] = user
                end
            end

            unless d["in_reply_to_user_id"].nil?
                user = { :@id =>  "#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}" }
                unless o[:users].empty?
                    #user = o[:users].select { |user| user[:@id] == "#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}" }.first
                    user = o[:users]["#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}"]
                end
                rdata[:recipient ] = user
            end
            
            # If it is a retweet; the user is not the creator of the message, is it only th sender ???                            
            ref_tweet_username = rdata[:name].scan(/^RT @[^:]*/)[0]
            unless ref_tweet_username.nil?
                ref_tweet_username = ref_tweet_username[4..-1].to_s
                user = o[:users].values.select { |user| user[:alternateName] == ref_tweet_username }.first
                rdata[:author] = user unless user.nil?
                rdata[:creator] = user unless user.nil?
            end

            # Expand geo/location
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
                        rdata[:contentLocation] = []
                        rdata[:contentLocation] << location
                    end
                end
            end

           
=begin                
                unless d["public_metrics"]["impression_count"].nil?
                    rdata[:interactionStatistic] << { 
                            "@type": "InteractionCounter",
                            "interactionType": "https://schema.org/????",
                            "userInteractionCount": d["public_metrics"]["impression_count"]
                        }
                end                              
=end


            # Expand referenced_tweets
            ref_tweet = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_referenced_tweets], d, ref_tweet, o)
            unless ref_tweet[:referenced_tweets].to_h[:identifier].nil?
                rdata[:identifier].concat( ref_tweet[:referenced_tweets][:identifier] )
            end
            unless ref_tweet[:referenced_tweets].to_h[:text].nil?
                rdata[:text] = ( ref_tweet[:referenced_tweets][:text] )
            end     
            unless ref_tweet[:referenced_tweets].to_h[:citation].nil?
                rdata[:citation] = ( ref_tweet[:referenced_tweets][:citation] )
            end             
            # out.clear
                    
            # Expand conversation
            conversation = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_conversation], d, conversation, o)
            
            unless conversation[:conversation].to_h.empty?
                rdata[:identifier] << conversation[:conversation].to_h[:identifier] 
                rdata[:isPartOf] = conversation[:conversation].to_h[:isPartOf] 
            end
            # conversation.clear




            #"entities": { "user_mentions": [] }
            tweet_expands = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_mentions], d, tweet_expands, o)            
            
            #"attachments": { "media_keys": [] }
            rules_ng.run(RULE_SET_v2_4[:rs_associated_media], d, tweet_expands, o)

            rdata.merge!(tweet_expands.to_h)

            rdata
        }]}
    },
    rs_basic_schema: {
        basic_schema: { "@" => lambda { |d,o|  
            {
                :@id            => "#{o[:prefixid]}_#{d["id"]}",
                :@type          => o[:type],
                :additionalType => "CreativeWork",
                :isBasedOn      => {
                    :@type    => "CreativeWork",
                    # @id must including dataset, Otherwise the records will be linked to all datasets in the graph
                    :@id      => "#{INGEST_CONF[:prefixid]}_#{INGEST_CONF[:provider][:@id] }_#{INGEST_CONF[:dataset][:@id]}",
                    :name     => INGEST_CONF[:genericRecordDesc],
                    :provider => INGEST_CONF[:provider],
                    :isPartOf => {
                        :@id   => INGEST_CONF[:dataset][:@id],
                        :@type => "Dataset",
                        :name  => INGEST_CONF[:dataset][:name],
                        :license  => INGEST_CONF[:dataset][:license]
                    }
                },
                :@context  => ["http://schema.org", { :@language => "#{  o[:contextLanguage] }-#{ INGEST_CONF[:unicode_script]}" }]    
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
                    unless o[:includes_tweets].nil?
                        #tweet = o[:includes_tweets].select { |tweet| tweet[:@id] == "#{o[:prefixid]}_#{ d["conversation_id"] }" }.first
                        #puts  o[:includes_tweets]
                        #puts  o[:includes_tweets].keys
                        tweet = o[:includes_tweets]["#{o[:prefixid]}_#{d["conversation_id"]}"]
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

            if d["url"] ==  "https://"
                u.except!(:url)
            end

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

    rs_referenced_tweets: { 
        referenced_tweets: { "@" => [ lambda { |d,o|  
            unless d["referenced_tweets"].nil?
                rdata = { :identifier => [] }
                d["referenced_tweets"].each { |referenced_tweet|
                    identifier = {
                        :@type => "PropertyValue", 
                        :value => referenced_tweet["id"]
                    }
                    case referenced_tweet["type"]
                        when "replied_to"
                            identifier[:@id]  = "replied_to_tweet_id_#{referenced_tweet["id"]}"
                            identifier[:name] = "replied_to_tweet_id"
                        when "quoted"
                            identifier[:@id]  = "quoted_tweet_id_#{referenced_tweet["id"]}"
                            identifier[:name] = "quoted_tweet_id"
                        when "retweeted"
                            identifier[:@id]  = "retweeted_tweet_id_#{referenced_tweet["id"]}"
                            identifier[:name] = "retweeted_tweet_id"
                    end
                    unless o[:includes_tweets].nil?
                        ref_tweet = o[:includes_tweets]["#{o[:prefixid]}_#{ referenced_tweet["id"] }"]
                    end
                    unless ref_tweet.nil?
                        identifier[:url] = "/#/record/#{o[:prefixid]}_#{referenced_tweet["id"]}"
                    end
                    
                    rdata[:identifier] << identifier

                    #"lang": "de"
                    out = DataCollector::Output.new
                    rules_ng.run(RULE_SET_v2_4[:rs_in_language], d, out, o)
                    rdata.merge!(out.to_h)

                    unless out.to_h[:inLanguage][:@id] == "und"
                        o[:contextLanguage] = out.to_h[:inLanguage][:@id]
                    else
                        o[:contextLanguage] = INGEST_CONF[:metaLanguage]
                    end

                    # Text of a retweeted tweet might be cut off (twitter API v1)
                    # Test of a qouted tweet will be set to citation
                    unless ref_tweet.nil?
                        if referenced_tweet["type"] == "retweeted"
                            rdata[:text] = ref_tweet[:text]
                        end
                        if referenced_tweet["type"] == "quoted"
                            rdata[:citation] = {
                                :@id            => "#{o[:prefixid]}_#{referenced_tweet["id"]}",
                                :@type          => o[:type],
                                :additionalType => "CreativeWork",
                                :@context       => ["http://schema.org", { :@language => "#{ o[:contextLanguage] }-#{ INGEST_CONF[:unicode_script]}" }],
                                :name           => ref_tweet[:text],
                                :inLanguage     => ref_tweet[:inLanguage],
                                :identifier     => ref_tweet[:identifier]
                            }
                        end
                    end
                }
            end
            rdata
            }]
        }
    },

    rs_includes_tweets: { 
        # tweet Object (d)
        # If a refferenced_tweet is also part of the dataset
        # It will also be available with author, creator, sender and recipoent
        includes_tweets: { "$.includes.tweets" => [ lambda { |d,o|  
            out = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_tweets], d, out, o)
            rdata = out[:tweets].to_h
            rdata[:isBasedOn].delete(:isPartOf)
            rdata.delete(:author)
            rdata.delete(:creator)
            rdata.delete(:sender)
            rdata.delete(:recipient)
            rdata
        }]}
    },
            
    rs_tweets: { 
    # tweet Object (d)
        tweets: { "@" => [ lambda { |d,o|  
            out = DataCollector::Output.new

            rdata = {
                :datePublished => d["created_at"],
                :identifier    => [ {
                    :@type => "PropertyValue", 
                    :@id   => "tweet_id_#{d["id"]}",
                    :name  => "tweet_id", 
                    :value => d["id"]} 
                ],
                :sameAs        => "https://twitter.com/temp/status/#{d["id"]}",
                :name          => d["text"],
                :text          => d["text"],
                :publisher     => {
                    :@type => "Organization",
                    :@id   => "iCANDID_ORGANIZATION_TWITTER",
                    :name  => "Twitter"
                }
            }

            #add id, isBasedOn, isPartOf
            #rules_ng.run(RULE_SET_v2_4[:rs_basic_schema], d, out, o)
            #out.clear
            
            user = {
                :@type => "Person",
                :@id   => "#{o[:prefixid]}_PERSON_#{d["author_id"]}"
            }

            rdata[:author]  = user
            rdata[:creator] = user
            rdata[:sender]  = user   
 
            unless d["in_reply_to_user_id"].nil?
                user = {
                    :@type => "Person",
                    :@id   => "#{o[:prefixid]}_PERSON_#{d["in_reply_to_user_id"]}"
                }
                rdata[:recipient ] = user
            end

            #"lang": "de"
            rules_ng.run(RULE_SET_v2_4[:rs_in_language], d, out, o)
            rdata.merge!(out.to_h)

            unless out.to_h[:inLanguage][:@id] == "und"
                o[:contextLanguage] = out.to_h[:inLanguage][:@id]
            else
                o[:contextLanguage] = INGEST_CONF[:metaLanguage]
            end

            


            basic_schema = DataCollector::Output.new
            rules_ng.run(RULE_SET_v2_4[:rs_basic_schema], d, basic_schema, o)
            rdata.merge!(basic_schema[:basic_schema].to_h)

            #"entities": { "hashtags": [] 
            rules_ng.run(RULE_SET_v2_4[:rs_keywords], d, out, o)
            rdata.merge!(out.to_h)

            # out.clear
            rdata

        }]}
    }
}
