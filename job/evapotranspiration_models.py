from pyspark.sql import SparkSession
import pyspark.sql.functions as F

from pyspark.ml.feature import VectorAssembler
from pyspark.ml.regression import LinearRegression

MONGODB_URI='mongodb://mongodb/'
def getCollection(sparksession):
	df = sparksession.read.format("com.mongodb.spark.sql.DefaultSource").load()
	return df
#metodo per riempire tutti valori nulli di colonne incluse nel parametro include con la media
def fill_with_mean(df, include=set()): 
    stats = df.agg(*(
        F.avg(c).alias(c) for c in df.columns if c in include
    ))
    return df.na.fill(stats.first().asDict())

spark = SparkSession.builder \
    .master("local") \
    .appName("pantheon") \
    .config("spark.mongodb.input.uri","mongodb://root:mongodb@mongodb/pantheon.station?authSource=admin") \
    .getOrCreate()
stazioneDf = getCollection(spark)
stazioneDf.printSchema()
#filtriamo solo i campi che interessano
stazioneDf = stazioneDf.select('data_ora','pioggia_mm','pressione_mbar','rad W/mq',
    'temp1_max','temp1_min','temp1_media','ur1_media','wind_speed_media')

nullWindEntries = stazioneDf.filter(stazioneDf.wind_speed_media.isNull()).count()
nullRadEntires = stazioneDf.filter(stazioneDf['rad W/mq'].isNull()).count()

if (nullWindEntries>0):
    stazioneDf = fill_with_mean(stazioneDf, ['wind_speed_media'])


#n.b per esprimere piu' condizioni in and o or bisogna racchiudere le condizioni tra parentesi
avgT=stazioneDf.filter((stazioneDf['rad W/mq'].isNotNull())& \
                   ((F.hour(stazioneDf['data_ora'])<=18)|(F.hour(stazioneDf['data_ora'])>5)))
avgT=stazioneDf.filter((stazioneDf['rad W/mq'].isNotNull())& \
                   ((F.hour(stazioneDf['data_ora'])<=18)|(F.hour(stazioneDf['data_ora'])>5)))

stats=avgT.select([F.mean('rad W/mq')]).first()
avgRadValue= stats.asDict()
avgRadValue = avgRadValue['avg(rad W/mq)']

if(nullRadEntires >0):
    stazioneDf = stazioneDf.withColumn('rad W/mq',F.when((stazioneDf['rad W/mq'].isNull())& ((F.hour(stazioneDf['data_ora'])>18)|\
                                               (F.hour(stazioneDf['data_ora'])<=5)),0)\
                               .when((stazioneDf['rad W/mq'].isNull())& ((F.hour(stazioneDf['data_ora'])<=18)|\
                                               (F.hour(stazioneDf['data_ora'])>5)),avgRadValue)\
                               .otherwise(stazioneDf['rad W/mq']))

#Et0 = 0.0393 Rs * sqrt(T+9.5)-0.19* Rs^0.6 * lat^0.15 +0.048 *(T+20)(1-UMI/100)* u2^0.7, formula di Valiantzas
LATITUDE=0.73
#Questa cella serve aggregare i valori per ora, in base da avere valori piu' realistici di evapotraspirazione
eachHourDf = stazioneDf.select(["data_ora","wind_speed_media","temp1_media","rad W/mq","ur1_media"])\
    .groupBy(F.to_date(stazioneDf['data_ora']).alias("data"),F.hour(stazioneDf['data_ora']).alias('ora')).agg({"wind_speed_media":"avg","rad W/mq":"avg","ur1_media":"avg"\
                                               ,"temp1_media":"avg"}).\
    withColumnRenamed('avg(rad W/mq)','Rs').withColumnRenamed('avg(temp1_media)','T')\
    .withColumnRenamed('avg(ur1_media)','RH').withColumnRenamed('avg(wind_speed_media)','u2')

eachHourDf = eachHourDf.select('data','ora','RH','Rs','u2','T')

#La radiazione solare viene convertita in MJ/day che e' l'unita di misura richiesta per la formula di Valiantzas
eachHourDf = eachHourDf.withColumn('Rs',eachHourDf['Rs']*0.0864)


eachHourDf = eachHourDf.withColumn('Et0',0.0393*eachHourDf['Rs']*((eachHourDf['T']+9.5)**0.5)-0.19*(eachHourDf['Rs']**0.6)\
                                   *(0.73**0.15)+0.048*(eachHourDf['T']+20)\
                                   *(1-(eachHourDf['RH']/100))*(eachHourDf['U2']**0.7))
# prendiamo le temperature medie, minime emassime come feature

vectorAssembler = VectorAssembler(inputCols = ['RH','Rs','T','u2'], outputCol = 'features')
vstazione_df = vectorAssembler.transform(eachHourDf)
vstazione_df = vstazione_df.select(['features', 'Et0'])
# divisione training set e test set
splits = vstazione_df.randomSplit([0.8,0.2])
train_df = splits[0]
test_df = splits[1]


lr = LinearRegression(featuresCol = 'features', labelCol='Et0', maxIter=20, regParam=0.3, elasticNetParam=0.8)
lr_model = lr.fit(train_df)
print("Coefficients: " + str(lr_model.coefficients))
print("Intercept: " + str(lr_model.intercept))

# calcolo del root means square. Siccome stiamo lavorando con dati della radiazione di ordini di grandezza
# molto alti, riteniamo accettabile un errore di circa 150

trainingSummary = lr_model.summary
print("RMSE: %f" % trainingSummary.rootMeanSquaredError)
print("r2: %f" % trainingSummary.r2)


test_result = lr_model.evaluate(test_df)
print("Root Mean Squared Error (RMSE) on test data = %g" % test_result.rootMeanSquaredError)
