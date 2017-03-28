from pyspark import SparkConf, SparkContext
import collections
import re
from pyspark.sql import *
from pyspark.ml.feature import StringIndexer, HashingTF, IDF, Tokenizer
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.mllib.evaluation import BinaryClassificationMetrics

conf = SparkConf().setMaster("local").setAppName("LogisticExample")
sc = SparkContext(conf = conf)

def mapParseLine(line):
    cols = line.split(',')
    sentiment = cols[0]
    review = cols[1]
    review = re.sub(r'[^0-9a-zA-Z\s]+', '', review).lower().strip()
    return Row(sentiment=sentiment, review=review)

movies_raw_data = sc.textFile('file:///D:/temp/rshah/spark-2.0.1-bin-hadoop2.7/datasets/SparkPythonDoBigDataAnalytics-Resources/SparkPythonDoBigDataAnalytics-Resources/movietweets.csv')
mapped_sentiments = movies_raw_data.map(mapParseLine)

# convert the RDD into Dataframe
review_DF = mapped_sentiments.toDF()

# split the data into training and test dataset
train_data, test_data = review_DF.randomSplit([0.7, 0.3])

### making sure that positive and negative sentiments are evenly split among trianing and test data set
#>>> train_data.filter("sentiment = 'negative'").count()
#41
#>>> train_data.filter("sentiment = 'positive'").count()
#35
#>>> test_data.filter("sentiment = 'positive'").count()
#15
#>>> test_data.filter("sentiment = 'negative'").count()
#9
###

# string indexer on the label
stringIndexer = StringIndexer(inputCol="sentiment", outputCol="label")

# tf idf on the reviews
tokenizer = Tokenizer(inputCol="review", outputCol="words")
hashingTF = HashingTF(inputCol=tokenizer.getOutputCol(), outputCol="hashed")
idf = IDF(inputCol=hashingTF.getOutputCol(), outputCol="features")

#define logistic model
lrModel = LogisticRegression(maxIter = 10, regParam=0.01)

# combine into pipeline
pipeline = Pipeline(stages=[stringIndexer, tokenizer, hashingTF, idf, lrModel])

# train using logistic regression
model = pipeline.fit(train_data)

# transform the test data
prediction = model.transform(test_data)

# check accuracy
predictionAndLabels = prediction.rdd.map(lambda lp: (lp.prediction, lp.label))
trainErr = predictionAndLabels.filter(lambda (v, p): v != p).count() / float(review_DF.count())
print("Training Error = " + str(trainErr))
