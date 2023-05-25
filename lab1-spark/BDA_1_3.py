

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))


# (key, value) = ((year-month, station) (temperature))
rdd = lines.map(lambda x: ((x[1][0:7], x[0]), float(x[3])))

# filter all data points with between dates
rdd = rdd.filter(lambda x: int(x[0][0][0:4])>=1960 or int(x[0][0][0:4])<=2014)

#1.3 Number of measurements with temperate>10 for each month
rdd = rdd.groupByKey().mapValues(lambda x: sum(x) / len(x))

# (key, value) = (year-month, station)
#count_stations = year_temperature.map(lambda x: (x[0], (x[1][0])))

# Only name unique stations for a month once
#count_stations = count_stations.distinct()

# Sum of entries for every key
#count_stations = count_stations.countByKey()
#count_stations = sc.parallelize(count_stations.items())


#print(max_temperatures.collect())


# Output for 1)
#count_temperatures.saveAsTextFile("BDA/output_1_2_1")
#Output for 2)
rdd.saveAsTextFile("BDA/output_1_3")
