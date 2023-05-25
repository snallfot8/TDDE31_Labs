

from pyspark import SparkContext

sc = SparkContext(appName = "exercise 1")
# This path is to the file on hdfs
ostergotland_file = sc.textFile("BDA/input/stations-Ostergotland.csv")
precipitation_file = sc.textFile("BDA/input/precipitation-readings.csv")
ost_station_lines = ostergotland_file.map(lambda line: line.split(";"))
prec_lines = precipitation_file.map(lambda line: line.split(";"))

# (key, value) = (station, temperature)
ost_stations_rdd = ost_station_lines.map(lambda x: x[0])
prec_rdd = prec_lines.map(lambda x: (x[0], (x[1], float(x[3]))))
prec_rdd = prec_rdd.filter(lambda x: int(x[1][0][0:4])>= 1993 and int(x[1][0][0:4]) <= 2016)

#Use broadcast to filter the ostergotland stations
ost_stations_list = ost_stations_rdd.collect()
ost_stations_list = sc.broadcast(ost_stations_list)

# only store prec_rdd if key in ost_stations_list
prec_rdd = prec_rdd.filter(lambda x: x[0] in ost_stations_list.value)

#Map ( (station,date), precipitation)
prec_rdd = prec_rdd.map(lambda x: ((x[0], x[1][0]), float(x[1][1])))
#Sum precipitation for each day
prec_rdd = prec_rdd.groupByKey().mapValues(lambda x: sum(x))
#Map so we have unique key for each month
prec_rdd = prec_rdd.map(lambda x: ((x[0][0], x[0][1][0:7]), float(x[1])))
# Sum of all days to get monthly average {station, year-month : average daily temp}
prec_rdd = prec_rdd.groupByKey().mapValues(lambda x: sum(x) )
# Map so we only look only at months
prec_rdd = prec_rdd.map(lambda x: (x[0][1], float(x[1]) ))
#Average by stations
prec_rdd = prec_rdd.groupByKey().mapValues(lambda x: sum(x) / len(x))

prec_rdd.saveAsTextFile("BDA/output_1_5")
