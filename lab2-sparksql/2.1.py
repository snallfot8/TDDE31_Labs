from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#Map (year, sation_number, temp)
year_temperature = lines.map(lambda x: Row(year=int(x[1][0:4]), station=x[0], temp=float(x[3])))

#create dataframe
df = sqlContext.createDataFrame(year_temperature)
df.registerTempTable("year_temperature")

df = df.filter( (df['year']<=2014) & (df['year']>=1950) ) #choose years

df_max = df.groupBy("year") \
    .agg(F.max("temp").alias("temp"))
#join with original df to get station
df_max = df_max.join(df, ["temp", "year"]).sort(F.desc("temp")) #order in descending order

df_min = df.groupBy("year") \
    .agg(F.min("temp").alias("temp"))
#join with original df to get station
df_min = df_min.join(df, ["temp", "year"]).sort(F.desc("temp")) #order in descending order


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
df_max.rdd.saveAsTextFile("BDA/output_2_1_max")
df_min.rdd.saveAsTextFile("BDA/output_2_1_min")
#df.write.text("BDA/output_2_1")
