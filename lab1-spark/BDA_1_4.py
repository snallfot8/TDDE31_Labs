

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
temp_lines = temperature_file.map(lambda line: line.split(";"))
prec_lines = precipitation_file.map(lambda line: line.split(";"))

# Groo

# (key, value) = (station, temperature)
temp_rdd = temp_lines.map(lambda x: (x[0], float(x[3])))
prec_rdd = prec_lines.map(lambda x: ( (x[0], x[1]), float(x[3])))

#1.4 Max temperature and max accumulated precipitation for each station
temp_rdd = temp_rdd.groupByKey().mapValues(lambda x: max(x))
prec_rdd = prec_rdd.groupByKey().mapValues(lambda x: sum(x))
prec_rdd = prec_rdd.map(lambda x: (x[0][0], float(x[1])))
prec_rdd = prec_rdd.groupByKey().mapValues(lambda x: max(x))

# Filter temperature between 25 and 30
# Filter precipitation 100mm and 200mm
temp_rdd = temp_rdd.filter(lambda x: x[1]>= 25.0 and x[1] <= 30.0)
prec_rdd = prec_rdd.filter(lambda x: x[1]>= 100.0 and x[1] <= 200.0)

# Merge data on stations {station : (temp, precipitation)}
rdd = temp_rdd.join(prec_rdd)

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
rdd.saveAsTextFile("BDA/output_1_4")
