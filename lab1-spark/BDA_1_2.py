from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
lines = temperature_file.map(lambda line: line.split(";"))

# (key, value) = (year-month, (station, temperature))
year_temperature = lines.map(lambda x: (x[1][0:7], (x[0],float(x[3]))))

#filter all data points with temp > 10
year_temperature = year_temperature.filter(lambda x: (int(x[0][0:4])>=1950 or int(x[0][0:4])<=2014) and float(x[1][1])>10.0)

#1.2.1 Number of measurements with temperate>10 for each month

#count_temperatures = year_temperature.countByKey()
#count_temperatures = sc.parallelize(count_temperatures.items())

#year_temperature = year_temperature.map(lambda x: (x,1))
#year_temperature = year_temperature.reduceByKey(lambda v1, v2: v1+v2)

# 1.2.2 Number of stations with measurements >10 for each month

# (key, value) = (year-month, station)

count_stations = year_temperature.map(lambda x: (x[0], (x[1][0])))

# Only name unique stations for a month once
count_stations = count_stations.distinct()

# Sum of entries for every key
count_stations = count_stations.countByKey()
count_stations = sc.parallelize(count_stations.items())


#print(max_temperatures.collect())


# Output for 1)
#count_temperatures.saveAsTextFile("BDA/output_1_2_1")
#Output for 2)
count_stations.saveAsTextFile("BDA/output_1_2_2")