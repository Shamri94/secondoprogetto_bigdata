#!/bin/bash
docker cp .\dati\nodi.json mongodb:/nodi.json
docker cp .\dati\stazione.json mongodb:/stazione.json
docker exec -it mongodb mongoimport --file nodi.json --db pantheon --collection nodes --jsonArray --username root --password mongodb --authenticationDatabase admin
docker exec -it mongodb mongoimport --file stazione.json --db pantheon --collection station --jsonArray --username root --password mongodb --authenticationDatabase admin