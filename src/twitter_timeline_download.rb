#encoding: UTF-8
$LOAD_PATH << '.' << './lib' << "#{File.dirname(__FILE__)}" << "#{File.dirname(__FILE__)}/lib"
ROOT_PATH = File.join( File.dirname(__FILE__), '../')

# Get userid by screen_name
# https://twitter.com/i/api/graphql/xmU6X_CKVnQ5lSrCbAmJsg/UserByScreenName?variables=%7B%22screen_name%22%3A%22br_sprecher%22%2C%22withSafetyModeUserFields%22%3Atrue%7D&features=%7B%22hidden_profile_subscriptions_enabled%22%3Atrue%2C%22rweb_tipjar_consumption_enabled%22%3Atrue%2C%22responsive_web_graphql_exclude_directive_enabled%22%3Atrue%2C%22verified_phone_label_enabled%22%3Afalse%2C%22subscriptions_verification_info_is_identity_verified_enabled%22%3Atrue%2C%22subscriptions_verification_info_verified_since_enabled%22%3Atrue%2C%22highlights_tweets_tab_ui_enabled%22%3Atrue%2C%22responsive_web_twitter_article_notes_tab_enabled%22%3Atrue%2C%22subscriptions_feature_can_gift_premium%22%3Afalse%2C%22creator_subscriptions_tweet_preview_api_enabled%22%3Atrue%2C%22responsive_web_graphql_skip_user_profile_image_extensions_enabled%22%3Afalse%2C%22responsive_web_graphql_timeline_navigation_enabled%22%3Atrue%7D&fieldToggles=%7B%22withAuxiliaryUserLabels%22%3Afalse%7D
# url = icandid_config.config[:timeline_url]

# https://api.twitter.com/2/users/#{{{userid}}}/tweets?exclude=retweets&start_time=2020-12-01T00:00:00Z&end_time=2021-06-01T00:00:00Z&expansions=author_id,referenced_tweets.id,referenced_tweets.id.author_id,entities.mentions.username,entities.note.mentions.username,attachments.poll_ids,attachments.media_keys,attachments.media_source_tweet,in_reply_to_user_id,geo.place_id,edit_history_tweet_ids&tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,referenced_tweets,reply_settings,source,text,withheld&media.fields=duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text,variants&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld,most_recent_tweet_id,public_metrics,verified_type&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type&poll.fields=duration_minutes,end_datetime,id,options,voting_status&max_results=100&
# https://twitter.com/search?q=%28from%3A+EU_Commission%29+until%3A2021-06-01+since%3A2021-01-10+-filter%3Areplies&src=recent_search_click


pp "####################################################################################"
pp ""
pp "            ISSUES WITH writing data[:data]                          "
pp ""
pp "            CHECK test/postprocessing_twitter.rb                     "
pp "####################################################################################"


require 'icandid_collector'
provider = 'twitter'

PROCESS_TYPE = "download"

ingestJson =  File.read(File.join(ROOT_PATH, "./config/#{provider}/ingest.cfg"))
Dir[  File.join( ROOT_PATH,"src/rules/#{provider.downcase}_*.rb") ].each {|file| pp file; require file; }

INGEST_DATA = JSON.parse(ingestJson, :symbolize_names => true)

@logger = Logger.new(STDOUT)
@logger.level = Logger::DEBUG

def process_queries(icandid_config)
    begin

        # https://api.twitter.com/2/users/311941092/tweets?tweet.fields=attachments,author_id,context_annotations,conversation_id,created_at,edit_controls,edit_history_tweet_ids,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld&expansions=author_id,referenced_tweets.id,in_reply_to_user_id,attachments.media_keys,attachments.poll_ids,geo.place_id,entities.mentions.username,referenced_tweets.id.author_id&media.fields=duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics&place.fields=contained_within,country,country_code,full_name,geo,id,name,place_type&poll.fields=voting_status&user.fields=created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,url,username,verified,withheld,public_metrics&start_time=2020-02-01T00:00:00Z&end_time=2021-06-01T00:00:00Z&max_results=5&exclude=retweets

        icandid_config.queries_to_process.each.with_index() do |query, index|

            start_processing = Date.today
            @logger.info ("Download records for query: #{ query[:query][:id] } [ #{ query[:query][:name] } ]")
            icandid_config.config[:query] = query    
            
            if query[:params].nil? || query[:params][:completed]
                next;
            end

            options = { 
                download_url_prop: "timeline_url"
            }

            icandid_config.prepare_query(query: query, options: options)
            url = icandid_config.config[ options[:download_url_prop].to_sym ]
            unless url.nil?

                @logger.info("download_url : #{url}")
                process_query(icandid_config: icandid_config, query: query, options: options)
    
            end

            query[:params][:last_run_update] = start_processing.strftime()
            icandid_config.update_query_config
        end
    end
end

def filter_twitter_error(errors)
    errors = [errors] if ! errors.is_a?(Array)

    #puts "============================================================="
    #pp errors
    #puts "============================================================="
    
    errors.select! { |e|
      !(e["title"] == "Forbidden" && e["resource_type"] == "user") &&
      !(e["title"] == "Not Found Error" && e["resource_type"] == "user") &&
      !(e["title"] == "Not Found Error" && e["resource_type"] == "place") &&
      !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "referenced_tweets.id") &&
      !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "edit_history_tweet_ids") &&      
      !(e["title"] == "Not Found Error" && e["resource_type"] == "tweet" && e["parameter"] == "attachments.media_source_tweet") &&      
      !(e["title"] == "Authorization Error" && e["resource_type"] == "tweet")
    }
    errors
  end
  


def process_query(icandid_config: nil, query: nil, options: {})

    if icandid_config.config[:rule_set].nil?
        raise "rule_set is required to parse file"
    else
        rule_set = icandid_config.config[ :rule_set].constantize 
    end

    icandid_config.ingest_data[:dataset][:@id]  = query[:query][:id]
    icandid_config.ingest_data[:dataset][:name] = query[:query][:name].gsub(/_/," ").capitalize()

    options[:prefixid] = "#{icandid_config.ingest_data[:prefixid]}_#{ icandid_config.ingest_data[:provider][:@id].downcase }_#{ icandid_config.ingest_data[:dataset][:@id].downcase }"

    icandid_config.update_config_with_query_data( query: query, options: options )
    url = icandid_config.config[ options[:download_url_prop].to_sym ]

    @logger.info ("Start Download query: #{ query[:query][:name] } ")
    @logger.info ("Start Download source_records_dir: #{ icandid_config.config[:source_records_dir] } ")
    
    while (url)
        
        @logger.info ("Start Download from url: #{ url } ")

        # url = "file:///source_records/Twitter/twitter_query_0000060/test_user_timeline.json"

        input_options = {
            bearer_token: icandid_config.config[:auth][:bearer_token],
            method: icandid_config.config[:method],
            number_of_retries: 5
        }
        
        icandid_input = IcandidCollector::Input.new( :icandid_config => icandid_config)
        data = icandid_input.collect_data_from_uri(url: url,  options: input_options )

        if data.nil?
            @logger.warn "NO DATA AVAILABLE on this url #{url}"
            break
        end

        output = DataCollector::Output.new

        rules_ng.run( rule_set[:rs_filename], data, output, options )
        rules_ng.run( rule_set[:rs_next_value], data, output, options )
        rules_ng.run( rule_set[:rs_raw_data], data, output, options )

        unless output["error"].nil?

            errors = filter_twitter_error( output["error"] )
            # pp "error error error error error error error error error error error error error error "
            # pp errors
            # pp "error error error error error error error error error error error error error error "

            unless errors.empty?
              url=nil
              @logger.error ("error in APi-response: #{ output["error"] } ")
              raise "Error in request"
            end
        end
        
        unless output["filename"].nil?
            filename = output["filename"].first
            file =  File.join( icandid_config.config[:source_records_dir], filename )
            icandid_output = IcandidCollector::Output.new( data: {data: data}, icandid_config: icandid_config)
            icandid_output.save_data_to_uri( uri: "file://#{file}" , options: {"content_type": "application/json"})
        end

        unless output["next_token"].nil?

            query[:params][:next_token] = output["next_token"].first 
            icandid_config.update_query_config
            query = icandid_config.queries_to_process.select{ |ptop| ptop[:internal_collector_id] == query[:internal_collector_id] }.first

            icandid_config.config[:query] = query
  
            icandid_config.update_config_with_query_data( query: query, options: options )
            url = icandid_config.config[:timeline_url]
            sleep 60;
        else
            query[:params][:completed] = true
            query[:params][:next_token] = nil
            # pp check for errors
            icandid_config.update_query_config
            url = nil
        end

    end
    
end


begin

    @logger = Logger.new(STDOUT)
    @logger.level = Logger::DEBUG
    @total_nr_parsed_records = 0    

    config = {
        :config_path => File.join(ROOT_PATH, "./config/#{provider}_timeline")
    }

    icandid_config = IcandidCollector::Configs.new( :config => config , :ingest_data => INGEST_DATA) 
    
    @logger.info ("Start downloading using config: #{ File.join( config[:config_path] , "config.yml") }")
    start_process  = Time.now.strftime("%Y-%m-%dT%H:%M:%SZ")
    @logger.info ("Download for queries in : #{File.join( icandid_config.query_config.path , icandid_config.query_config.name) }")

    process_queries(icandid_config)

rescue => exception
    @logger.error("Error : #{ exception } ")
ensure
    puts "Todo : send mail ?"
end






