from pyspark import SparkConf, SparkContext
import collections
import re

def removeAlphaNumericCharsAndLower(word):
    word = re.sub(r'\W+', ' ', word)
    return word.lower().strip()

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf = conf)

raw_data = sc.textFile('D:\\temp\\rshah\\spark-2.0.1-bin-hadoop2.7\\datasets\\Book.txt')
words = raw_data.map(removeAlphaNumericCharsAndLower).flatMap(lambda row: row.split()).map(lambda word: (word, 1)).reduceByKey(lambda x,y: x+y)

top5 = words.takeOrdered(5, key=lambda x: -x[1])
#results = words.collect()

print "++++++++++++++++++"
print "word count problem"
for key, value in top5:
    print key, value
print "++++++++++++++++++"

