from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.sql import *
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.mllib.evaluation import RegressionMetrics

sql = SQLContext(sc)

def mapParseLine(line):
    cols = line.split(',')
    label = float(cols[0])
    features = float(cols[1])
    return (label, Vectors.dense(features))

inputData = sc.textFile('file:///D:/temp/rshah/spark-2.0.1-bin-hadoop2.7/datasets/regression.txt')
col_names = ["label", "features"]
dataset = inputData.map(mapParseLine).toDF(col_names)

splitData = dataset.randomSplit([0.70, 0.30])
trainingData = splitData[0]
testData = splitData[1]

lr = LinearRegression(maxIter=10, regParam=0.3, elasticNetParam=0.8)

# Fit the model
lrModel = lr.fit(trainingData)

# Print the coefficients and intercept for linear regression
print("Coefficients: " + str(lrModel.coefficients))
print("Intercept: " + str(lrModel.intercept))

transformed = lrModel.transform(testData)
label_and_pred = transformed.rdd.map(lambda x: (float(x[2]), float(x[0])))





