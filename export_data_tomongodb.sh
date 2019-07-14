python3 csv_to_json_convert.py
mongoimport --db pantheon --collection nodes --file data/nodi.json --jsonArray
mongoimport --db pantheon --colection station --file data/stazione.json --jsonArray
