import boto3
import re

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
    return list(regex.sub('',word) for word in line.lower().split())
    
def lambda_handler(event, context):
    s3 = boto3.client("s3")
    if event:
        fileObject = event["Records"][0]
        fileName = str(fileObject["s3"]["object"]['key'])
        fileObject2 = s3.get_object(Bucket = "csci5253-fall2018-project3-serverless-muthukrishnan-upadhyaya", Key = fileName)
        fileContent = fileObject2["Body"].read().decode('UTF-8')
        strippedLowerWords = list(filter(lambda x: x not in stopwords and x not in '', funToLower(fileContent)))
        s = boto3.resource('s3')
        #s.objects.all().delete()
        o = s.Object("csci5253-fall2018-project3-serverless-muthukrishnan-upadhyaya-o", fileName)
        objectToPut = " ".join(word for word in strippedLowerWords)
        o.put(Body = objectToPut)
    return "Done"
