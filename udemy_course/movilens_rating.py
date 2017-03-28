from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("movilensRating")
sc = SparkContext(conf = conf)

def parseMoviesData(line):
    cols = line.split('|')
    movie_id = int(cols[0])
    movie_name = cols[1]
    return (movie_id, movie_name)

ratings_data = sc.textFile('D:\\temp\\rshah\\spark-2.0.1-bin-hadoop2.7\\datasets\\ml-100k\\u.data')
ratings = ratings_data.map(lambda x: x.split()[1]).map(lambda x: (int(x),1))

result = ratings.reduceByKey(lambda x,y: x+y).sortBy(lambda x: x[1])

movie_data = sc.textFile('D:\\temp\\rshah\\spark-2.0.1-bin-hadoop2.7\\datasets\\ml-100k\\u.item')
movies = movie_data.map(parseMoviesData)
movies_lu = dict(movies.collect())
movies_broadcast = sc.broadcast(movies_lu)

result_w_movie_names = result.map(lambda (movie_id, count): (movies_broadcast.value[movie_id], count)).collect()

for s in result_w_movie_names:
    print s


