from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    cols = line.split()
    age = int(cols[2])
    friends = int(cols[3])
    return (age, friends)

conf = SparkConf().setMaster("local").setAppName("computeAverage")
sc = SparkContext(conf = conf)

social_data = sc.textFile('D:\\temp\\rshah\\spark-2.0.1-bin-hadoop2.7\\datasets\\sample.txt')
reqd_cols = social_data.map(parseLine)

transformed = reqd_cols.mapValues(lambda x: (x,1))
new = transformed.reduceByKey(lambda x,y: (x[0]+y[0],x[1]+y[1]))
new1 = new.mapValues(lambda x: (1.0*x[0]/x[1])).collect()

print "++++++++++++++++++"
print "Compute Average Friends by Age"
for result in new1:
    print result
print "++++++++++++++++++"

