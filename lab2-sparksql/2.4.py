from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
temp = temperature_file.map(lambda line: line.split(";"))
prec = precipitation_file.map(lambda line: line.split(";"))

#Map (year, sation_number, temp)
temp = temp.map(lambda x: Row(station=x[0], temp=float(x[3])))
prec = prec.map(lambda x: Row(station = x[0], year=int(x[1][0:4]),
    month=int(x[1][5:7]), day = int(x[1][8:10]), prec = float(x[3])))

#create dataframe
df_temp = sqlContext.createDataFrame(temp)
df_prec = sqlContext.createDataFrame(prec)
df_temp.registerTempTable("temp")
df_prec.registerTempTable("prec")

#sum daily precepitation
df_prec = df_prec.groupby("year", "month", "day", "station") \
    .agg(F.sum("prec").alias("prec"))

#filter temperatures
df_temp = df_temp.groupby("station") \
    .agg(F.max("temp").alias("temp")).filter("temp > 25.0 and temp < 30.0")

#filer precipitation
df_prec = df_prec.groupby("station") \
    .agg(F.max("prec").alias("prec")).filter("prec > 100.0 and prec < 200.0")

df = df_temp.join(df_prec, "station").sort(F.desc("prec"))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
df.rdd.saveAsTextFile("BDA/output_2_4")
