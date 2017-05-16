import time
import datetime
from pyspark import SparkContext
from pyspark import SparkConf

from pyspark.sql import SparkSession

spark = SparkSession \
    .builder \
    .appName("Python Spark SQL basic example") \
    .config("spark.some.config.option", "some-value") \
    .getOrCreate()

conf = SparkConf().setMaster("local").setAppName("Project 2")
sc = SparkContext(conf = conf)

tweetsRdd = sc.textFile("tweets4.txt")

tweetsDataFrame = tweetsRdd.map(lambda line: line.split('\n')).toDF()
tweetsDataFrame.printSchema()

tweetsDataFrame.createOrReplaceTempView("tweets")

sqlDF = spark.sql("SELECT * FROM tweets")




# filtered = students.filter(lambda student: student[2] == "71381" and student[5] == "F")

# filtered.collect()

# one_minute = 30*100

# s = "Mon May 15 21:54:06 +0000 2017"

# w = time.mktime(datetime.datetime.strptime(s, "%a %b %d %H:%M:%S +0000 %Y").timetuple()) + 10*one_minute

# print(w)

# utc_dt = datetime.datetime.fromtimestamp(w).strftime('%a %b %d %H:%M:%S +0000 %Y')

# print(utc_dt)

# print(s)

# tweets = map(lambda tweetString: json.loads(tweetString), tweetStrings)

