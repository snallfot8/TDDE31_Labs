from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#Map (year, sation_number, temp)
year_temperature = lines.map(lambda x: Row(year=int(x[1][0:4]),
    month=int(x[1][5:7]),  station=x[0], temp=float(x[3])))

#create dataframe
df = sqlContext.createDataFrame(year_temperature)
df.registerTempTable("year_temperature")

#choose years and filter out temp<10
df = df.filter( (df['year']<=2014) & (df['year']>=1960) )

#Take average temperature for each month and station
df = df.groupby("year", "month", "station") \
    .agg(F.mean("temp").alias("avg(temp)")) \
    .sort(F.desc("avg(temp)"))

# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
df.rdd.saveAsTextFile("BDA/output_2_3")
