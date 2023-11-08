Twitter=( twitter_query_0000001 twitter_query_0000002 twitter_query_0000003 twitter_query_0000004 twitter_query_0000005 twitter_query_0000006 twitter_query_0000007 twitter_query_0000008 twitter_query_0000009 twitter_query_0000010 twitter_query_0000011 twitter_query_0000012 twitter_query_0000013 twitter_query_0000014 twitter_query_0000015 )
Twitter=( twitter_query_0000008 )

periodes=( 2023 2022 2021 2020 2019 2018 2017 2016 2015 2014 2013 2012 2011 2010 2009 2008 2007 2006 )
periodes=( 2020 )


start_time="2000-01-01 01:00:00"


cd /app/

for t in ${periodes[@]}; do
 for q in ${Twitter[@]}; do
  logfile="/app/logs/twitter_parser_backlog_${q}_${t}.log"
  sourcedir="/source_records/Twitter/${q}/**/backlog/${t}_*/**/"
  recordsdir="/records/Twitter/$q/test/"
  pattern="*.json"
  config_file="/app/config/twitter/config_icandid3.yml"

  echo "Parsing Twitter ${q} recent ${t}"
  echo "config: ${config_file}"
  echo "sourcedir: ${sourcedir}"
  echo " Check for errors in /icandid/icandid/volumes/collector_v2_2/${logfile}"

  bash_command="ruby ./src/twitter_parser.rb -c ${config_file} --query_id $q -s ${sourcedir} -q /app/config/twitter/queries.yml -u $start_time -p ${pattern} -d ${recordsdir} > ${logfile}"

  echo -e "run \n  ${bash_command}\n"
  ${bash_command}
  
  

 done
done
