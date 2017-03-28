from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("computeMinTemperature")
sc = SparkContext(conf = conf)

weather_data = sc.textFile('D:\\temp\\rshah\\spark-2.0.1-bin-hadoop2.7\\datasets\\1800.csv')
weather_data_splitted = weather_data.map(lambda row: row.split(','))
weather_data_filtered = weather_data_splitted.filter(lambda x: x[2] == 'TMIN')

weather_data_min_temp = weather_data_filtered.map(lambda x: (x[0], int(x[3]))).reduceByKey(lambda x,y: min(x,y))

results = weather_data_min_temp.collect()

print "++++++++++++++++++"
print "Compute min Temp"
for result in results:
    print result
print "++++++++++++++++++"

