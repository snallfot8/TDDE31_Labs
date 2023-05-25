from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
ost_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ost = ost_file.map(lambda line: line.split(";"))
prec = precipitation_file.map(lambda line: line.split(";"))

#Map (year, sation_number, temp)
ost = ost.map(lambda x: Row(station=x[0]))
prec = prec.map(lambda x: Row(station = x[0], year=int(x[1][0:4]),
    month=int(x[1][5:7]), day = int(x[1][8:10]), prec = float(x[3])))

#create dataframe
df_ost = sqlContext.createDataFrame(ost)
df_prec = sqlContext.createDataFrame(prec)
df_ost.registerTempTable("ost")
df_prec.registerTempTable("prec")

df_prec = df_prec.filter( (df_prec['year']<=2016) & (df_prec['year']>=1993) )


#sum daily precepitation for each day
df_prec = df_prec.groupby("year", "month", "station") \
    .agg(F.sum("prec").alias("prec"))

#inner join with Ostergotland stations
df = df_prec.join(df_ost, "station")

#calculate average precipitation for stations
df = df.groupby("year", "month") \
    .agg(F.mean("prec").alias("prec"))
    .sort(F.desc("prec"))


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
df.rdd.saveAsTextFile("BDA/output_2_5")
