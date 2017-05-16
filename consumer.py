from pathlib import Path

# Import kafka packages
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Import Spark packages
from pyspark import SparkContext
from pyspark import SparkSession
from pyspark.streaming import StreamingContext

try:
    import json
except ImportError:
    import simplejson as json

# Create a local StreamingContext with two working thread and batch interval of 10 minutes
sc = SparkContext(appName="TweetMachine")

spark = SparkSession.builder.appName("TweetMachineThing").getOrCreate()

keyWordsRdd = sc.emptyRDD()
hashtagsRdd = sc.emptyRDD()
screenNameRdd = sc.emptyRDD()
trumpWordsRdd = sc.emptyRDD()

proccessTask1And2 = False
batchCounter = 0
batchCounter2 = 0
batchCounter3 = 0

ssc = StreamingContext(sc, 600)

batchCounter +=  1
batchCounter2 +=  1
batchCounter3 +=  1

# Create a DStream that will connect to hostname:port, like localhost:9092
tweetStrings = ssc.socketTextStream("localhost", 9092)
open("tempFile1.txt", "w+").close()
open("tempFile2.txt", "w+").close()
open("tempFile3.txt", "w+").close()
open("tempFile4.txt", "w+").close()
for tweet in tweetStrings:
    tweetJson = json.loads(tweet)
    if 'delete' not in tweetJson:
        # Get Screennames
        tempString = str("%s,%s" % (tweetJson["created_at"], tweetJson["user"]["screen_name"]))
        tempFile = open("tempFile1.txt", "a+")
        tempFile.write("%s\n" % tempString)
        tempFile.close()

        #Get words
        words = tweetJson.replace(",", "").replace(".", "").replace("!", "").replace("?", "").split()
        for word in words:
            tempString = str("%s,%s" % (tweetJson["created_at"], word))
            tempFile = open("tempFile2.txt", "a+")
            tempFile.write("%s\n" % tempString)
            tempFile.close()

            #Get Hashtags
            if word.startswith("#"):
                tempString = str("%s,%s" % (tweetJson["created_at"], word))
                tempFile = open("tempFile3.txt", "a+")
                tempFile.write("%s\n" % tempString)
                tempFile.close()

            #Get Trump words
            if word.lower() == "trump" or word.lower() == "maga" or word.lower() == "dictator" or word.lower() == "impeach" or word.lower() == "drain" or word.lower() == "swamp":
                tempString = str("%s,%s" % (tweetJson["created_at"], word))
                tempFile = open("tempFile4.txt", "a+")
                tempFile.write("%s\n" % tempString)
                tempFile.close()

tempRdd = sc.textFile("tempFile.txt").map(lambda line: line.split('\n'))
mainRdd.union(tempRdd)

if(batchCounter == 6):
    proccessTask1And2 = True

if proccessTask1And2:
    if batchCounter == 1:
        #Erase last 10 minutes of data for task 1 and 2
    
    # do task 1 and 2
    keywordDataFrame = keyWordsRdd.toDF(["date", "word"])
    hashtagsDataFrame = hashtagsRdd.toDF(["date", "word"])

    keywordDataFrame.createOrReplaceTempView("keywords")
    hashtagsDataFrame.createOrReplaceTempView("hashtags")

    keywordResult = spark.sql("SELECT TOP 10 word, count(word) as occurence FROM keywords GROUP BY word ORDER BY occurence")
    hashtagsResult = spark.sql("SELECT TOP 10 word, count(word) as occurence FROM hashtags GROUP BY word ORDER BY occurence")

    resultString = ""
    for hashtag in hashtagsResult.collect():
        resultString += hashtag[1] + ","

    resultFile = open("result1.txt", "a+")
    resultFile.write("%s\n" % resultString[:-1])

    resultString = ""
    for keyword in keywordResult.collect():
        resultString += keyword[1] + ","

    resultFile = open("result2.txt", "a+")
    resultFile.write("%s\n" % resultString[:-1])

    if(batchCounter % 6 == 0):
        if batchCounter2 == 72:
            #Erase last hour of data for task 3
            batchCounter2 = 0

        if batchCounter3 == 144:
            #Erase last hour of data for task 4
            batchCounter3 = 0

        batchCounter = 0
        # do task 3 and 4
        screennameDataFrame = screenNameRdd.toDF(["date", "screen_name"])
        trumpWordsDataFrame = trumpWordsRdd.toDF(["date", "word"])

        screennameDataFrame.createOrReplaceTempView("screen_names")
        trumpWordsDataFrame.createOrReplaceTempView("trumps")

        screennameResult = spark.sql("SELECT TOP 10 screen_name, count(screen_name) as occurence FROM screen_names GROUP BY screen_name ORDER BY occurence")
        trumpWordsResult = spark.sql("SELECT TOP 10 word, count(word) as occurence FROM trumps GROUP BY word ORDER BY occurence")

        resultString = ""
        for screen_name in screennameResult.collect():
            resultString += screen_name[1] + ","

        resultFile = open("result3.txt", "a+")
        resultFile.write("%s\n" % resultString[:-1])

        resultString = ""
        for trumpWord in trumpWordsResult.collect():
            resultString += trumpWord[1] + ","

        resultFile = open("result4.txt", "a+")
        resultFile.write("%s\n" % resultString[:-1])


ssc.start()             # Start the computation