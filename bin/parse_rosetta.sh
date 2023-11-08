
#!/bin/bash

logfile="./logs/rosetta_parser.log"
echo "Parsing Rosetta"
echo " Check for errors in /icandid/icandid/volumes/collector_v2_2/${logfile}"

bash_command="ruby ./src/rosetta_parser.rb -u "2000-02-02" -p IE*000.xml > ${logfile}"
bash_command="ruby ./src/rosetta_parser.rb -u "2000-02-02" -n affiches_query_0000001 -p IE*0.xml"
#bash_command="ruby ./src/rosetta_parser.rb -u "2000-02-02" -n fotoalbums_query_0000001 -p IE*0.xml"
#bash_command="ruby ./src/rosetta_parser.rb -u "2000-02-02" -p IE*.xml"
echo "=> cd ~/icandid"
echo "=> /usr/local/bin/docker-compose run --rm icandid_collector_v2_4"
echo "=>  ${bash_command}"
cd ~/icandid;
/usr/local/bin/docker-compose run -u 10000 --rm icandid_collector_v2_4 /bin/sh -c "${bash_command}"
