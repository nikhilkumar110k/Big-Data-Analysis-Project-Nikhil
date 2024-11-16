from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from collections import namedtuple
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns

# Initialize SparkContext and StreamingContext
sc = SparkContext()
ssc = StreamingContext(sc, 10)
sqlcontext = SQLContext(sc)

# Create a socket stream
socket_stream = ssc.socketTextStream("127.0.0.1", 9999)
lines = socket_stream.window(20)

# Define a named tuple for storing hashtag counts
fields = ("tag", "count")
tweet = namedtuple('tweet', fields)

# Process the stream
hashtags = (
    lines.flatMap(lambda text: text.split(" "))
    .filter(lambda word: word.lower().startswith("#"))
    .map(lambda word: (word.lower(), 1))
    .reduceByKey(lambda a, b: a + b)
)

# Output operation: Save each RDD as a DataFrame and query it
def process_rdd(rdd):
    if not rdd.isEmpty():
        # Convert the RDD to a DataFrame
        df = sqlcontext.createDataFrame(rdd, fields)
        df.createOrReplaceTempView("tweets")
        
        # Query top hashtags
        top_10 = sqlcontext.sql("SELECT tag, count FROM tweets ORDER BY count DESC LIMIT 10")
        top_10_df = top_10.toPandas()
        
        # Plot using Seaborn
        if not top_10_df.empty:
            display.clear_output(wait=True)
            plt.figure(figsize=(10, 8))
            sns.barplot(x="count", y="tag", data=top_10_df, palette="viridis")
            plt.title("Top 10 Hashtags")
            plt.show()

hashtags.foreachRDD(process_rdd)

# Start the StreamingContext
ssc.start()

# Await termination
ssc.awaitTermination()
