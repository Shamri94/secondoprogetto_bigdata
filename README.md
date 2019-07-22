# secondoprogetto_bigdata
Secondo progetto per il corso di Big Data presso l'università Roma Tre

Per avviare l'applicazione basta scaricare la repository, posizionarsi nella root e avviare il comando **docker-compose up**
Verrà avviata un'applicazione con 5 container:
  - Jupyter notebook con Spark
  - Mongodb
  - PostgreSQL
  - Adminer (interfaccia web per db relazionali)
  - Mongo-express (interfaccia web per MongoDB)


Per poter effettivamente lavorare con i notebook bisogna importare i dati su Mongodb, eseguendo lo script _saveDatasetToMongoDB.sh_ e 
avendo i file salvati nella repository nella cartella **data** quindi non presente.
Per poter usare Jupyter bisogna controllare la console di Docker e cercare nel log il token di accesso, dopodiché basta entrare sul 
browser su **localhost:8888**.
Per poter interagire su Postgres basta andare sull'URL **localhost:8085** e specificare come server _postgres_ che è il nome di
dominio del container con Postgres.Lo username  è _postgres_ e la password _password_. Bisogna creare un database di nome **report** per poi poter eseguire i notebook e salvare i risultati
