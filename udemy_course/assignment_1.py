from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Assignment1")
sc = SparkContext(conf = conf)

def parseLine(line):
    columns = line.split(',')
    customer_id = int(columns[0])
    amount = float(columns[2])
    return (customer_id, amount)

raw_data = sc.textFile('file:///D:/temp/rshah/spark-2.0.1-bin-hadoop2.7/datasets/customer-orders.csv')
results = raw_data.map(parseLine).reduceByKey(lambda x,y: x+y).sortBy(lambda x: -x[1]).collect()

for key, value in results:
    print key,  value
