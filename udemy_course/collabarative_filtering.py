#Boilerplate stuff:
from pyspark import SparkConf, SparkContext
from math import sqrt
import sys

conf = SparkConf().setMaster("local").setAppName("AwesomeRecommendationEngine")
sc = SparkContext(conf = conf)

def parseLine(line):
    cols = line.split()
    user_id = int(cols[0])
    movie_id = int(cols[1])
    rating = float(cols[2])
    return (user_id, (movie_id, rating))

def parseLineNew(line):
    cols = line.split('::')
    user_id = int(cols[0])
    movie_id = int(cols[1])
    rating = float(cols[2])
    return (user_id, (movie_id, rating))

def filterDuplicates(data):
    user_id = data[0]
    ratings = data[1]
    movie1 = ratings[0][0]
    movie2 = ratings[1][0]
    rating1 = ratings[0][1]
    rating2 = ratings[1][1]
    return (movie1 != movie2 and rating1 != rating2 and movie1 < movie2)

def getMovieGenre(line):
    cols = line.split('|')
    movie_id = cols[0]
    unknown = int(cols[5])
    action = int(cols[6])
    adventure = int(cols[7])
    animation = int(cols[8])
    children = int(cols[9])
    comedy = int(cols[10])
    crime = int(cols[11])
    documentary = int(cols[12])
    drama = int(cols[13])
    fantasy = int(cols[14])
    film_noir = int(cols[15])
    horror = int(cols[16])
    musical = int(cols[17])
    mystery = int(cols[18])
    romance = int(cols[19])
    sci_fi = int(cols[20])
    thriller = int(cols[21])
    war = int(cols[22])
    western = int(cols[23])
    results = []
    if(unknown == 1):
        results.append((movie_id, 'unknown'))
    if(action == 1):
        results.append((movie_id, 'action'))
    if(adventure == 1):
        results.append((movie_id, 'adventure'))
    if(animation == 1):
        results.append((movie_id, 'animation'))
    if(children == 1):
        results.append((movie_id, 'children'))
    if(comedy == 1):
        results.append((movie_id, 'comedy'))
    if(crime == 1):
        results.append((movie_id, 'crime'))
    if(documentary == 1):
        results.append((movie_id, 'documentary'))
    if(drama == 1):
        results.append((movie_id, 'drama'))
    if(fantasy == 1):
        results.append((movie_id, 'fantasy'))
    if(film_noir == 1):
        results.append((movie_id, 'film_noir'))
    if(horror == 1):
        results.append((movie_id, 'horror'))
    if(musical == 1):
        results.append((movie_id, 'musical'))
    if(mystery == 1):
        results.append((movie_id, 'mystery'))
    if(romance == 1):
        results.append((movie_id, 'romance'))
    if(sci_fi == 1):
        results.append((movie_id, 'sci_fi'))
    if(thriller == 1):
        results.append((movie_id, 'thriller'))
    if(war == 1):
        results.append((movie_id, 'war'))
    if(western == 1):
        results.append((movie_id, 'western'))
    return results

def getMovieGenreNew(line):
    cols = line.split('::')
    movie_id = cols[0]
    genre_list = cols[2]
    results = []
    for genre in genre_list.split('|'):
        if('unknown' == genre.lower() or '(no genres listed)' == genre.lower()):
            results.append((movie_id, 'unknown'))
        if('action' == genre.lower()):
            results.append((movie_id, 'action'))
        if('adventure' == genre.lower()):
            results.append((movie_id, 'adventure'))
        if('animation' == genre.lower()):
            results.append((movie_id, 'animation'))
        if('children' == genre.lower()):
            results.append((movie_id, 'children'))
        if('comedy' == genre.lower()):
            results.append((movie_id, 'comedy'))
        if('crime' == genre.lower()):
            results.append((movie_id, 'crime'))
        if('documentary' == genre.lower()):
            results.append((movie_id, 'documentary'))
        if('drama' == genre.lower()):
            results.append((movie_id, 'drama'))
        if('fantasy' == genre.lower()):
            results.append((movie_id, 'fantasy'))
        if('film-noir' == genre.lower()):
            results.append((movie_id, 'film_noir'))
        if('horror' == genre.lower()):
            results.append((movie_id, 'horror'))
        if('musical' == genre.lower()):
            results.append((movie_id, 'musical'))
        if('mystery' == genre.lower()):
            results.append((movie_id, 'mystery'))
        if('romance' == genre.lower()):
            results.append((movie_id, 'romance'))
        if('sci-fi' == genre.lower()):
            results.append((movie_id, 'sci_fi'))
        if('thriller' == genre.lower()):
            results.append((movie_id, 'thriller'))
        if('war' == genre.lower()):
            results.append((movie_id, 'war'))
        if('western' == genre.lower()):
            results.append((movie_id, 'western'))
        if('imax' == genre.lower()):
            results.append((movie_id, 'imax'))
    return results
    

def filterSameGenreMovies(dataset):
    movie1_values = dataset[1]
    movie2_values = dataset[2]
    movie1_genres = movie1_values[1]
    movie2_genres = movie2_values[1]
    for i in movie1_genres:
        if i in movie2_genres:
            return True
    return False

def loadMovieNames():
    movieNames = {}
    with open('D:\temp\rshah\spark-2.0.1-bin-hadoop2.7\datasets\ml-10M100K\movies.dat') as f:
        for line in f:
            fields = line.split('::')
            movieNames[int(fields[0])] = fields[1].decode('ascii', 'ignore')
    return movieNames
    
def extractReqdCols(line):
    user_id = line[0]
    movie1 = line[1]
    movie2 = line[2]
    ratings1 = movie1[0]
    ratings2 = movie2[0]
    return (user_id, (ratings1, ratings2))

def makePairs(movies):
    (movie1, rating1) = movies[1][0]
    (movie2, rating2) = movies[1][1]
    return ((movie1, movie2), (rating1, rating2))

def computeCosineSimilarity(ratingPairs):
    numPairs = 0
    sum_xx = sum_yy = sum_xy = 0
    for ratingX, ratingY in ratingPairs:
        sum_xx += ratingX * ratingX
        sum_yy += ratingY * ratingY
        sum_xy += ratingX * ratingY
        numPairs += 1
    numerator = sum_xy
    denominator = sqrt(sum_xx) * sqrt(sum_yy)
    score = 0
    if (denominator):
        score = (numerator / (float(denominator)))
    return (score, numPairs)



ratings_data = sc.textFile('file:///D:/temp/rshah/spark-2.0.1-bin-hadoop2.7/datasets/ml-10M100K/ratings.dat')

# parse the data and only filter to good ratings
user_mapped = ratings_data.map(parseLineNew).filter(lambda (user, in_tuple): in_tuple[1] > 2.0).partitionBy(25)

# self join to get the movies rated by the same user
self_joined = user_mapped.join(user_mapped)

# remove all the duplicates from self join
filter_dups = self_joined.filter(filterDuplicates)

#get the genres of the movies
movies_data = sc.textFile('file:///D:/temp/rshah/spark-2.0.1-bin-hadoop2.7/datasets/ml-10M100K/movies.dat').cache()
name_dict = dict(movies_data.map(lambda line: line.split('::')).map(lambda fields: (int(fields[0]), fields[1])).collect())
genre_data = movies_data.flatMap(getMovieGenreNew)

# only keep pairs of the movies of the same genres

#broadcasting the genre dataset
genre_dict = dict(genre_data.groupByKey().collect())
genre_broadcast = sc.broadcast(genre_dict)

#adding genre of the movies as the one more data to the tuple
map_movies_to_genre = filter_dups.map(lambda (x,(y1, y2)): \
                                      (x,(y1, genre_broadcast.value[str(y1[0])]), (y2, genre_broadcast.value[str(y2[0])])))
#only keeping movies from the same genre
same_genre_movies = map_movies_to_genre.filter(lambda line: filterSameGenreMovies(line))

#extracting required cols (removing the genre): now the data structure is: (user_id, (ratings1, ratings2))
same_genres_mapped = same_genre_movies.map(extractReqdCols)

#converting to format: ((movie1, movie2), (rating1, rating2))
made_pairs = same_genres_mapped.map(makePairs)

#in order to compute the similarity , grouping by keys
pairs_grouped = made_pairs.groupByKey()

#get the scores
moviePairSimilarities = pairs_grouped.mapValues(computeCosineSimilarity)


# Extract similarities for the movie we care about that are "good".
if (len(sys.argv) > 1):

    scoreThreshold = 0.97
    coOccurenceThreshold = 1000

    movieID = int(sys.argv[1])

    # Filter for movies with this sim that are "good" as defined by
    # our quality thresholds above
    filteredResults = moviePairSimilarities.filter(lambda((pair,sim)): \
        (pair[0] == movieID or pair[1] == movieID) \
        and sim[0] > scoreThreshold and sim[1] > coOccurenceThreshold)

    # Sort by quality score.
    results = filteredResults.map(lambda((pair,sim)): (sim, pair)).sortByKey(ascending = False).take(10)

    print("************************************************")
    print("Top 10 similar movies for " + name_dict[movieID])
    for result in results:
        (sim, pair) = result
        # Display the similarity result that isn't the movie we're looking at
        similarMovieID = pair[0]
        if (similarMovieID == movieID):
            similarMovieID = pair[1]
        print(name_dict[similarMovieID] + "\tscore: " + str(sim[0]) + "\tstrength: " + str(sim[1]))
    print("************************************************")

