import pandas as pd
import json  


stazione_df = pd.read_csv('data/pantheon20190612-stazione.csv',sep=';')
nodi_df= pd.read_csv('data/pantheon20190612-nodi.csv',sep=';')
stazione_df.to_json(r'data/stazione.json',orient='records')
nodi_df.to_json(r'data/nodi.json',orient='records')
