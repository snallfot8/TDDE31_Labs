from __future__ import division
from math import radians, cos, sin, asin, sqrt, exp
from datetime import datetime
from pyspark import SparkContext
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.tree import DecisionTree, DecisionTreeModel, RandomForest, RandomForestModel
from pyspark.mllib.linalg import Vectors

sc = SparkContext(appName="lab_kernel")
temperature_file = sc.textFile("BDA/input/temperature-readings.csv")
stations_file = sc.textFile("BDA/input/stations.csv")

def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """

    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])
    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat/2)**2 + cos(lat1) * cos(lat2) * sin(dlon/2)**2
    c = 2 * asin(sqrt(a))
    km = 6367 * c
    return km

h_distance = 100
h_date = 10
h_time = 2
#latitude and longitude for Danderyd
a = 59.4113 #latitude
b = 18.0578 #longitude
predDate = datetime(2013,7,4) # Up to you

# Your code here

temp_lines = temperature_file.map(lambda line: line.split(";"))
station_lines = stations_file.map(lambda line: line.split(";"))
stations_rdd = station_lines.map(lambda x: (x[0], (float(x[3]), float(x[4]))))

#Use broadcast to filter the stations
stations_bc = stations_rdd.collectAsMap()
stations_bc = sc.broadcast(stations_bc)

# rdd(Station, (date, time, temp, latitude, longitude))
rdd = temp_lines.map(lambda x: (x[0], (datetime(int(x[1][0:4]), int(x[1][5:7]), int(x[1][8:10])), int(x[2][0:2]), float(x[3]),
    stations_bc.value[x[0]][0], stations_bc.value[x[0]][1])))

rdd = rdd.sample(False, 0.05) #only take 10%

# Creating the distance kernel
def distanceKernel(predLat, predLong, dataLat, dataLong, h):
    u = haversine(predLong, predLat, dataLong, dataLat)/h
    return(exp(-u**2))

# Creating the day kernel
def dayKernel(predDate, dataDate, h):
    d = (predDate-dataDate).days % 365
    if (d>365/2):
        d = 365-d
    u = d/h
    return(exp(-u**2))

# Creating the time kernel
def timeKernel(predHour, dataHour, h):
    d = abs(predHour-dataHour)
    if(d>12):
        d = 24-d
    u = d/h
    return(exp(-u**2))

#filter out dates before our prediction dates
rdd = rdd.filter(lambda x: (x[1][0] < predDate))
rdd.cache()

##----------------ML---------------------------##

# RDD = (Temp, [year, month, day, time, latitude, longitude])
rdd_ml = rdd.map(lambda x: (x[1][2], (x[1][0].year, x[1][0].month, x[1][0].day, x[1][1], x[1][3], x[1][4])))
iterations = 100
step_size = 1.0
numTrees=5
maxDepth=10
cat = {}
train_data = rdd_ml.map(lambda x: LabeledPoint(x[0], x[1]))
ran_reg_model = RandomForest.trainRegressor(train_data, categoricalFeaturesInfo={}, numTrees=numTrees, maxDepth=maxDepth)
dec_tree_model = DecisionTree.trainRegressor(train_data, categoricalFeaturesInfo={}, maxDepth = maxDepth)

result_ran = []
result_dec = []

for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    time_int = int(time[0:2])
    data = [predDate.year, predDate.month, predDate.day, time_int, a,b]
    result_ran.append((time, ran_reg_model.predict(data)))
    result_dec.append((time, dec_tree_model.predict(data)))

result_ran = sc.parallelize(result_ran)
result_dec = sc.parallelize(result_dec)
result_ran.coalesce(1).saveAsTextFile("BDA/output_3_ran")
result_dec.coalesce(1).saveAsTextFile("BDA/output_3_dec")

##------------------Kernels--------------------##

#run kernel functions, save rdd(time, temp, sumKernel, sumKernel*temp, prodKernel, prodKernel*temp)

rdd = rdd.map(lambda x: (x[1][1], x[1][2],
    (distanceKernel(a, b, x[1][3], x[1][4], h_distance)+dayKernel(predDate, x[1][0], h_date)),
    ((distanceKernel(a, b, x[1][3], x[1][4], h_distance)+dayKernel(predDate, x[1][0], h_date))*x[1][2]),
    (distanceKernel(a, b, x[1][3], x[1][4], h_distance)*dayKernel(predDate, x[1][0], h_date)),
    ((distanceKernel(a, b, x[1][3], x[1][4], h_distance)*dayKernel(predDate, x[1][0], h_date))*x[1][2])))

newMap = True

for time in ["24:00:00", "22:00:00", "20:00:00", "18:00:00", "16:00:00", "14:00:00",
"12:00:00", "10:00:00", "08:00:00", "06:00:00", "04:00:00"]:
    time_int = int(time[0:2])
    rdd_temp = rdd.filter(lambda x: (x[0] < time_int))
    rdd_temp.cache()
    #run kernel functions, save rdd(1, temp, sumKernel, prodKernel)
    rdd_temp = rdd_temp.map(lambda x:(1,
        ((x[2]+timeKernel(time_int, x[0], h_time)),
        (x[3]+(timeKernel(time_int, x[0], h_time)*x[1])),
        (x[4]*timeKernel(time_int, x[0], h_time)),
        (x[5]*timeKernel(time_int, x[0], h_time)))))

#     Make sure sum of kernels equals 1
    rdd_temp = rdd_temp.reduceByKey(lambda x,y: ((x[0]+y[0]),(x[1]+y[1]),(x[2]+y[2]),(x[3]+y[3])))
    rdd_temp = rdd_temp.mapValues(lambda x: ((x[1]/x[0]), (x[3]/x[2])))
    predictions.append(rdd_temp.collect())

    if (newMap):
        kernel_sum = rdd_temp.map(lambda x: (time, x[1][0]))
        kernel_prod = rdd_temp.map(lambda x: (time, x[1][1]))
        newMap = False
    else:
        kernel_sum = kernel_sum.union(rdd_temp.map(lambda x: (time, x[1][0])))
        kernel_prod = kernel_prod.union(rdd_temp.map(lambda x: (time, x[1][1])))

#Save the results to text file
kernel_sum.coalesce(1).saveAsTextFile("BDA/output_3_sum")
kernel_prod.coalesce(1).saveAsTextFile("BDA/output_3_prod")
