
# Usage
docker compose run --rm sav_to_csv bach "python spss_to_csv.py -i path/to/input.sav [-o path/to/output.csv]"

docker compose run --rm sav_to_csv python spss_to_csv.py -i /data/Thema_042022-2023.sav
docker compose run --rm sav_to_csv python spss_to_csv.py -i /data/Actor_42022-2023.sav

docker compose run --rm sav_to_csv python spss_to_csv.py -i /data/Thema_042022-2023.sav Thema_all.csv
docker compose run --rm sav_to_csv python spss_to_csv.py -i /data/Actor_42022-2023.sav Actoren_all.csv

docker compose run --rm sav_to_csv /data/splitter.sh