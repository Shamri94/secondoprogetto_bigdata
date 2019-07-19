from pyspark.sql import SparkSession

from pyspark.sql import SparkSession

MONGODB_URI='mongodb://127.0.0.1/'
def getCollection(dbName,collectionName):
	spark = SparkSession \
	    .builder \
	    .appName("myApp") \
	    .config("spark.mongodb.input.uri",MONGODB_URI+dbName+'.'+collectionName) \
	    .getOrCreate()
	df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
	return df

stazioneDf = getCollection('pantheon','stations')
stazioneDf.printSchema()


