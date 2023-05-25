from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year,temperature)
year_temperature = lines.map(lambda x: (x[1][0:4], (x[0],float(x[3]))))

#filter
year_temperature = year_temperature.filter(lambda x: int(x[0])>=1950 or int(x[0])<=2014)

max_temperatures = year_temperature.reduceByKey(lambda a,b: (a[0], max(a[1], b[1])))
max_temperatures = max_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

min_temperatures = year_temperature.reduceByKey(lambda a,b: (a[0], min(a[1], b[1])))
min_temperatures = min_temperatures.sortBy(ascending = False, keyfunc=lambda k: k[1])

#print(max_temperatures.collect())


# Output for 1)
max_temperatures.saveAsTextFile("BDA/output_max")
min_temperatures.saveAsTextFile("BDA/output_min")
