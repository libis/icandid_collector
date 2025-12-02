
cd /app/src/;


CONFIG="/app/config/tiktok/config.yml"
QUERY_CONFIG="/app/config/tiktok/queries/config_finish_refugees.yml"
LASTTIME_PARSED="2020-01-01"
SOURCE_DIR="/source_records/test/"
RECOURS_DIR="/records/test/"
RECORD_PATTERN="tiktok_6954.*774.json"
QUERY_IDR="tiktok_query_0000004"

ruby /app/src/tiktok_parser.rb -c ${CONFIG} -q ${QUERY_CONFIG} -s ${SOURCE_DIR} -d ${RECOURS_DIR} -u ${LASTTIME_PARSED} -p ${RECORD_PATTERN}

