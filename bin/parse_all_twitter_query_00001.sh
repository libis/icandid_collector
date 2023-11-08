ruby ./src/twitter_parser.rb -c ./config/twitter/config.yml -q ./config/twitter/queries.yml -u "2000-01-01 01:00:00" --query_id "twitter_query_00001" -p "*.json"
ruby ./src/twitter_parser.rb -c ./config/twitter/config.yml -q ./config/twitter/queries.yml -u "2000-01-01 01:00:00" --query_id "twitter_query_00002" -p "*.json"


ruby ./src/twitter_download.rb -c ./config/twitter/config.yml -q ./config/twitter/queries.yml -u "2000-01-01 01:00:00"
     --query_id "twitter_query_00018" -p "*.json"
