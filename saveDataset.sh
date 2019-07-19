#!/bin/bash
sudo docker cp ./data/nodi.json mongodb:/nodi.json
sudo docker cp ./datas/tazione.json mongodb:/stazione.json
sudo docker exec -it mongodb mongoimport --file nodi.json --db pantheon --collection nodes --jsonArray --username root --password mongodb --authenticationDatabase admin
sudo docker exec -it mongodb mongoimport --file stazione.json --db pantheon --collection station --jsonArray --username root --password mongodb --authenticationDatabase admin