from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import desc
from collections import namedtuple
from regex import split as split
import time
from IPython import display
import matplotlib.pyplot as plt
import seaborn as sns



sc= SparkContext()

ssc= StreamingContext(sc,10)
sqlcontext= SQLContext(sc)

socket_stream= ssc.socketTextStream("127.0.0.1",9999)
lines=socket_stream.window(20)
fields=("tags","count")
tweet= namedtuple('tweet',fields)

(lines.flatMap(lambda text: text.split( " " )).
 filter(lambda word:word.lower().startswith("#")).
 map(lambda word:(word.lower(),1))
 .reduceByKey(lambda a,b: a+b))

ssc.start()

count=0

while count<0:
    time.sleep(3)
    top_10= sqlcontext.sql('select tag, count from tweets: ')
    top_10_df=top_10.toPandas()
    display.clear_output(wait=True)
    sns.plt.figure(figsize=(10,8))
    sns.barplot(x="count",y="tag",data=top_10_df)
    sns.plt.show()
    count=count + 1
    
ssc.stop()