from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from collections import namedtuple
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns


sc = SparkContext()
ssc = StreamingContext(sc, 10)
sqlcontext = SQLContext(sc)


socket_stream = ssc.socketTextStream("127.0.0.1", 9999)
lines = socket_stream.window(20)


fields = ("tag", "count")
tweet = namedtuple('tweet', fields)


hashtags = (
    lines.flatMap(lambda text: text.split(" "))
    .filter(lambda word: word.lower().startswith("#"))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
)


def process_rdd(rdd):
    if not rdd.isEmpty():
       
        df = sqlcontext.createDataFrame(rdd, fields)
        df.createOrReplaceTempView("tweets")
        
      
        top_10 = sqlcontext.sql("SELECT tag, count FROM tweets ORDER BY count DESC LIMIT 10")
        top_10_df = top_10.toPandas()
        
       
        if not top_10_df.empty:
            display.clear_output(wait=True)
            plt.figure(figsize=(10, 8))
            sns.barplot(x="count", y="tag", data=top_10_df, palette="viridis")
            plt.title("Top 10 Hashtags")
            plt.show()

hashtags.foreachRDD(process_rdd)


ssc.start()


ssc.awaitTermination()
