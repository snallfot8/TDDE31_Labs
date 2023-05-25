from pyspark import SparkContext
from pyspark.sql import SQLContext, Row
from pyspark.sql import functions as F

sc = SparkContext()
sqlContext = SQLContext(sc)

# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

#Map (year, sation_number, temp)
year_temperature = lines.map(lambda x: Row(year=int(x[1][0:4]), month=int(x[1][5:7]),  station=x[0], temp=float(x[3])))

#create dataframe
df = sqlContext.createDataFrame(year_temperature)
df.registerTempTable("year_temperature")

#choose years and filter out temp<10
df = df.filter( (df['year']<=2014) & (df['year']>=1950) & (df["temp"]>=10.0) )

#count number of readings for each month
df_readings = df.groupBy("year", "month").count() \ #count number of readings
    .orderBy("count", ascending=False) #order in descending order

#count number of stations that had a reading >10 degrees
df_distinct_readings = df["year", "month", "station"].drop_duplicates() \
    .groupBy("year", "month").count() \ #count number of stations
    .orderBy("count", ascending=False) #order in descending order


# Following code will save the result into /user/ACCOUNT_NAME/BDA/output folder
df_readings.rdd.saveAsTextFile("BDA/output_2_2_readings")
df_distinct_readings.rdd.saveAsTextFile("BDA/output_2_2_dist_readings")
