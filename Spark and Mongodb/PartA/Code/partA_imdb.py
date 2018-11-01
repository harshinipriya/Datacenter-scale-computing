import re
from pyspark import SparkContext
sc = SparkContext("local", "Simple App")

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

rdd = sc.textFile("imdb/input")
rdd.take(5)

top_words_list = rdd.flatMap(funToLower).filter(lambda x: x not in stopwords and x not in '').map(lambda x:(x,1)).reduceByKey(lambda x,y: x+y).sortByKey().top(2000,key=lambda x:x[1])#.map(lambda x:(x[1],x[0])).top(2000)
#with open("PartB/Part2/Bucket" + str(rating) + "/output.txt" ,'w') as fileName:
with open("output.txt",'w') as fileName:
    for word in top_words_list:
        fileName.write("%s\t%s\n" %(word[0], word[1]))
