#encoding: UTF-8
require 'data_collector'
require "iso639"
require_relative 'basic_schema'

@rule_set_name = "RULE_SET_v0_1"

RULE_SET_v0_1 = {
    version: "0.1",
    rs_filename:{
        filename: { "$" => lambda { |d,o| 
            "#{ d["data"].first["created_utc"]}_#{d["data"].last["created_utc"]}.json"
        }}
    },  
    rs_next_value:{
        next_token: { "$" => lambda { |d,o|
            unless d.empty?
                d["data"].last["created_utc"]
            end
        }}
    },    
    rs_raw_data:{
        data: { "$.data" => lambda { |d,o| 
                d
            }
        },
        metadata: { "$" => lambda { |d,o|
                # De downloadtijd moet worden toegevoegd, aangezien dit later belangrijk kan zijn voor het parsen, 
                # bijvoorbeeld om de periode van de interactionStatistic te bepalen.
                d["metadata"].nil? ? rdata = {} : rdata = d["metadata"]
                rdata[:download_time] = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
                rdata[:download_url]  = o[:download_url]
                
                rdata
            }
        },
        error: { "$.error" => lambda { |d,o| 
            d
            }
        }
    },
    rs_expand_with_comments:{
        expand_with_comments: { "@" => lambda { |d,o| 

            comments = []
            comments_per_request = 100
            link_id = d["id"]
            before_created_utc = ""
            url = "https://api.pullpush.io/reddit/search/comment/?link_id=#{link_id}&sort_type=created_utc&before=#{before_created_utc}&sort=desc&size=#{comments_per_request}"

            input_options = {
                number_of_retries: 5
                
            }
            icandid_input = IcandidCollector::Input.new( :icandid_config => {} )

            while url 
                comments_data = icandid_input.collect_data_from_uri(url: url,  options: input_options )
                comments.concat(comments_data["data"])

                if comments_data["data"].size < comments_per_request
                    pp "ALL COMMENTS ARE DOWNLOADED"
                    url=nil
                    break
                else
                    before_created_utc = comments_data["data"].last["created_utc"] - 1
                    url = "https://api.pullpush.io/reddit/search/comment/?link_id=#{link_id}&sort_type=created_utc&before=#{before_created_utc}&sort=desc&size=#{comments_per_request}"
                end
                sleep rand(20)
               
            end
            d["comments"] = comments
            d
        }}
    },
    rs_id:{
        id:  {'$.id' =>  lambda { |d,o| 
            d
        }}
    },
    rs_records: {
        records: { "$" => [ lambda { |d,o| 
                
            if @rule_set_name.is_a?(String)
                @rule_set_name = eval(@rule_set_name)
            end
            
            o[:downloadtime] = d["metadata"]["download_time"]
            
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
        record: { "$.data" => lambda { |d,o|

            rdata = {}

            out = DataCollector::Output.new

            rules_ng.run(@rule_set_name[:rs_id], d, out, o)
            o[:id] = out[:id]

            if  o[:id].nil?
                raise "No id found in d #{d}"
            end
            
            # pp "=================================> o[:id] #{o[:id]}"
            rules_ng.run(RULE_SET_BASIC_ICANDID[:rs_basic_schema], d, out, o)
            rdata.merge!(out[:basic_schema].to_h.symbolize_keys)
            out.clear

            # user text language
            o[:index] = 0
            rules_ng.run(@rule_set_name[:rs_record_data], d, out, o)
            rdata.merge!(out.to_h.symbolize_keys)
            out.clear


            pp "====================== o[:id] #{o[:id]} ===========> sameAs #{ rdata[:sameAs] }"
            
            if rdata[:creator][:@id] == "reddit_deleted_user"
                unless rdata[:text].nil? || rdata[:text] == "[deleted]"
                    # Moet de text worden verwijderd als de user niet meer bestaat ?
                    # pp "TEXT TEXT TEXT"
                    # pp rdata[:text]
                    # raise "No creator" if rdata[:creator][:@id] == "reddit_deleted_user"
                end
                if rdata[:text].nil?
                    rdata[:text] == "[deleted]"
                end
            end

            rdata[:author] = rdata[:creator]
            rdata[:sender] = rdata[:creator]
            rdata.compact
            
        } }
    },
    rs_record_data:{
        name: '$.title',
        creator: { "$" =>  lambda { |d,o| 
            # raise "No author_fullname avalable" if d["author_fullname"].nil? || d["author_fullname"].empty?
            if d["author"] == "[deleted]"
                { 
                    :@id  => "reddit_deleted_user", 
                    :name => d["author"], 
                    :url  => "https://www.reddit.com/user/#{d["author"]}/"
                }
            else                
                { 
                    :@id  => d["author_fullname"], 
                    :name => d["author"], 
                    :url  => "https://www.reddit.com/user/#{d["author"]}/"
                }
            end
        }},
        text: {'$' =>  lambda { |d,o| 
            if ! d["selftext"].nil? && ! d["body"].nil?
                pp "body"
                pp d["body"]
                pp "selftext"
                pp d["selftext"]
                exit
            end
            if d["body"].nil? && d["selftext"] != "[deleted]"
                d["selftext"]
            else
                d["body"]
            end
        }},   
        datePublished: {'$.created_utc' =>  lambda { |d,o| 
            Time.at(d).strftime("%Y-%m-%dT%H:%M:%S")
        }},
        isPartOf:  {'$' =>  lambda { |d,o| 
            {
                :@type => "Collection",
                :@id   => "https://www.reddit.com/r/#{d["subreddit"]}/",
                :name  => {
                    :@language => "en-Latn",
                    :@value => d["subreddit"]
                }
            }
        }},
        sameAs:  {'$' =>  lambda { |d,o| 
            "https://www.reddit.com#{d["permalink"]}"
        }},        
        interactionStatistic: { "$" => lambda { |d,o|
            rdata = []
            if (d["ups"] || 0) > 0
                rdata << { 
                        :@type                => "InteractionCounter",
                        :interactionType      => "https://schema.org/LikeAction",
                        :userInteractionCount => d["ups"],
                        :endTime              => o[:downloadtime]
                    }
            end
            if (d["downs"] || 0) > 0
                rdata << { 
                        :@type                => "InteractionCounter",
                        :interactionType      => "https://schema.org/DislikeAction",
                        :userInteractionCount => d["downs"],
                        :endTime              => o[:downloadtime]
                    }
            end
            rdata unless rdata.empty?
        }},
        inLanguage: { "@" =>  lambda { |d,o| 
            d = "en"
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
        comment:  { "$.comments" => lambda { |d,o|
            unless d["author_fullname"].nil? &&  d["text"].nil? && d["title"].nil?
                rdata= { :id => "#{o[:ingest_data][:prefixid]}_#{o[:ingest_data][:provider][:@id].downcase}_#{o[:id]}_#{d["id"]}" }
                out = DataCollector::Output.new
                rules_ng.run( @rule_set_name[:rs_record_data], d, out, o )
                rdata.merge(out.to_h.symbolize_keys)
            end      
        }}
    }
}
