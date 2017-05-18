#from pathlib import Path

# Import Spark packages
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
import requests
import json


try:
    import json
except ImportError:
    import simplejson as json

def VerifyNotDelete(tweet):
    if 'delete' not in tweet:
       return tweet

def VerifyHashtag(word):
    if word.startswith("#"):
        return word

def VerifyNotUnicode(word):
    if isinstance(word, unicode):
        return
    else:
        return word


def VerifyNotStopWord(word):
    if len(word)>3 and word not in ['a','about','above','after','again','against','all','am','an','and','any','are','as','at','be','because','been','before','being','below','between','both','but','by','cannot','could','did','do','does','doing','down','during','each','few','for','from','further','had','has','have','having','he','her','here','hers','herself','him','himself','his','how','i','if','in','into','is','it','its','itself','me','more','most','my','myself','no','nor','not','of','off','on','once','only','or','other','ought','our','ours','ourselves','out','over','own','same','she','should','so','some','such','than','that','the','their','theirs','them','themselves','then','there','these','they','this','those','through','to','too','under','until','up','very','was','we','were','what','when','where','which','while','who','whom','why','with','would','you','your','yours','yourself','yourselves']:
        return word

def VerifyTrumpWord(word):
    if word.upper() in ['TRUMP', 'MAGA', 'DICTATOR', 'IMPEACH', 'SWAMP', 'DRAIN', 'CHANGE']:
        return word.upper()

def PrepareServerForScreenNames(trash):
    print "Marroneo Intenso - Screennames"
    requests.post("http://selias.co.in/BigData/PrepareScreenNames", data={"val":True})

def PrepareServerForKeywords(trash):
    print "Marroneo Intenso - Keywords"
    requests.post("http://selias.co.in/BigData/PrepareKeyWords", data={"val":True})

def PrepareServerForHashtags(trash):
    print "Marroneo Intenso - Hashtags"
    requests.post("http://selias.co.in/BigData/PrepareHashtags", data={"val":True})

def PrepareServerForTrumpWords(trash):
    print "Marroneo Intenso - Trump words"
    requests.post("http://selias.co.in/BigData/PrepareTrumpWords", data={"val":True})

def SendScreenName(jsonData):  
    jsonString = "{screen_name:'%s',count:%s}" % (jsonData[0],jsonData[1])
    url = "http://selias.co.in/BigData/ScreenName?json=%s" % (jsonString)
    requests.get(url)

def SendKeyword(jsonData):   
    jsonString = "{word:'%s',count:%s}" % (jsonData[0],jsonData[1])
    url = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
    print url
    requests.get(url)

def SendHashtag(jsonData):   
    jsonString = "{hashtag:'%s',count:%s}" % (jsonData[0].replace("#", ""),jsonData[1])
    url = "http://selias.co.in/BigData/Hashtag?json=%s" % (jsonString)
    requests.get(url)

def SendTrumpWord(jsonData):   
    jsonString = "{word:'%s',count:%s}" % (jsonData[0],jsonData[1])
    url = "http://selias.co.in/BigData/TrumpWord?json=%s" % (jsonString)
    requests.get(url)

if __name__ == "__main__":
    sc = SparkContext(appName="TweetMachine")

    # Create a local StreamingContext with two working thread and batch interval of 10 minutes
    ssc = StreamingContext(sc, 30)

    sc.setCheckpointDir("/tmp/checkpoints/")

    consumer = KafkaUtils.createStream(ssc,"localhost:2181","twitter-streaming",{'tweets':1})

    data = consumer.map(lambda tweets: json.loads(tweets[1])) 

    wordsRdd = data.filter(VerifyNotDelete).filter(VerifyNotUnicode).flatMap(lambda tweet: tweet['text'].replace(",", "").replace(".", "").replace("!", "").replace("?", "").replace("-", "").replace("\t", " ").replace("\n", " ").split())

    # Keywords
    keywordsCounted = wordsRdd.filter(VerifyNotStopWord).countByValueAndWindow(3600,30).transform(lambda rdd: rdd.sortBy(lambda row: row[1],ascending=False))
    topKeywords = keywordsCounted.transform(lambda rdd:sc.parallelize(rdd.take(10)))
    hack = topKeywords.countByValueAndWindow(3600,30).transform(lambda rdd:sc.parallelize(rdd.take(0)))
    hack.foreachRDD(PrepareServerForKeywords)
    hack.pprint()
    topKeywords.foreachRDD(lambda row: row.foreach(SendKeyword))
    topKeywords.pprint()

    # Hashtags   
    hashtagsCounted = wordsRdd.filter(VerifyHashtag).countByValueAndWindow(3600,30).transform(lambda rdd: rdd.sortBy(lambda row: row[1],ascending=False))
    topHashtags = hashtagsCounted.transform(lambda rdd:sc.parallelize(rdd.take(10)))
    hack = topHashtags.countByValueAndWindow(3600,30).transform(lambda rdd:sc.parallelize(rdd.take(0)))
    hack.foreachRDD(PrepareServerForHashtags)
    hack.pprint()
    topHashtags.foreachRDD(lambda row: row.foreach(SendHashtag))
    topHashtags.pprint()

    # Screen Names
    screenNameRdd = data.filter(VerifyNotDelete).map(lambda tweet: tweet['user']['screen_name']) 
    screenNamesCounted = screenNameRdd.countByValueAndWindow(43200, 30).transform(lambda rdd: rdd.sortBy(lambda row: row[1], ascending=False))
    topScreenNames = screenNamesCounted.transform(lambda rdd:sc.parallelize(rdd.take(10)))
    hack = topScreenNames.countByValueAndWindow(43200,30).transform(lambda rdd:sc.parallelize(rdd.take(0)))
    hack.foreachRDD(PrepareServerForScreenNames)
    hack.pprint()
    topScreenNames.foreachRDD(lambda row: row.foreach(SendScreenName))
    topScreenNames.pprint()

    # Trump Words
    trumpWordsCounted = wordsRdd.filter(VerifyTrumpWord).countByValueAndWindow(86400,3600).transform(lambda rdd: rdd.sortBy(lambda row: row[1],ascending=False))
    hack = topScreenNames.countByValueAndWindow(43200,30).transform(lambda rdd:sc.parallelize(rdd.take(0)))
    hack.foreachRDD(PrepareServerForKeywords)
    hack.pprint()
    trumpWordsCounted.foreachRDD(lambda row: row.foreach(SendTrumpWord))
    trumpWordsCounted.pprint()
    
    ssc.start()             # Start the computation
    ssc.awaitTermination()