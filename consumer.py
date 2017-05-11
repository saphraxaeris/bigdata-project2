from pathlib import Path

# Import kafka packages
from kafka import KafkaConsumer
from kafka.errors import KafkaError

# Import Spark packages
from pyspark import SparkContext
from pyspark.streaming import StreamingContext

def get_tweets():
    # Create a local StreamingContext with two working thread and batch interval of 10 minutes
    sc = SparkContext(appName="NetworkTweetGet")
    ssc = StreamingContext(sc, 600)

    # Create a DStream that will connect to hostname:port, like localhost:9092
    tweetStrings = ssc.socketTextStream("localhost", 9092)
    
    # Append tweets to file
    tweetFile=open("tweets.txt", "a+")
    for tweet in tweetStrings:
        tweetFile.write("%s\n" % tweet)

    ssc.start()             # Start the computation

if __name__ == "__main__":
    print("Starting consumer...")
    get_tweets()