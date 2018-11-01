from pyspark.sql import SparkSession
import re

spark = SparkSession \
    .builder \
    .appName("myApp") \
    .config("spark.mongodb.input.uri", "mongodb://student:student@ec2-54-210-44-189.compute-1.amazonaws.com/test.reviews") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql").load()
df.printSchema()

myStopWordsList = ["a", "about", "above", "after", "again", "against", "all", "am", "an", 
"and", "any", "are", "aren't", "as", "at", "be", "because", "been", "before", "being", "below",
"between", "both", "but", "by", "can't", "cannot", "could", "couldn't", "did", "didn't", "do", "does",
"doesn't", "doing", "don't", "down", "during", "each", "few", "for", "from", "further", "had", "hadn't",
"has", "hasn't", "have", "haven't", "having", "he", "he'd", "he'll", "he's", "her", "here", "here's",
"hers", "herself", "him", "himself", "his", "how", "how's", "i", "i'd", "i'll", "i'm", "i've", "if",
"in", "into", "is", "isn't", "it", "it's", "its", "itself", "let's", "me", "more", "most", "mustn't",
"my", "myself", "no", "nor", "not", "of", "off", "on", "once", "only", "or", "other", "ought", "our",
"ours", "ourselves", "out", "over", "own", "same", "shan't", "she", "she'd", "she'll", "she's",
"should", "shouldn't", "so", "some", "such", "than", "that", "that's", "the", "their", "theirs", "them",
"themselves", "then", "there", "there's", "these", "they", "they'd", "they'll", "they're", "they've",
"this", "those", "through", "to", "too", "under", "until", "up", "very", "was", "wasn't", "we", "we'd",
"we'll", "we're", "we've", "were", "weren't", "what", "what's", "when", "when's", "where", "where's",
"which", "while", "who", "who's", "whom", "why", "why's", "with", "won't", "would", "wouldn't", "you",
"you'd", "you'll", "you're", "you've", "your", "yours", "yourself", "yourselves"]

regex = re.compile('[^a-zA-Z]')
def processStopwords():
    return set(regex.sub('',word) for word in myStopWordsList)
stopwords = processStopwords()

def funToLower(line):
    return list(regex.sub('',x) for x in line.lower().split())


for rating in range(1,6):
    reviews_df = df.filter(df['overall'] == rating).select("reviewText")
    reviews_df.show()
    reviews_rdd = reviews_df.rdd
    reviews_rdd.take(5)
    review_text_rdd = reviews_rdd.flatMap(list)
    review_text_rdd.take(5)
    top_words_list = review_text_rdd.flatMap(funToLower).filter(lambda x: x not in stopwords and x not in '').map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortByKey().top(500,key=lambda x:x[1])
    with open("Bucket" + str(rating) + "/output.txt" ,'w') as fileName:
        for word in top_words_list:
            fileName.write("%s\t%s\n" %(word[0], word[1]))
#with open("output" + str(rating) + ".txt",'w') as fileName:
    