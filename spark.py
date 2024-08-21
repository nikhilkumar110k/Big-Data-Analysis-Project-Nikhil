from pyspark.sql import SparkSession
spark=SparkSession.builder.appName("BDA Project").getOrCreate()
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.functions import when

df= spark.read.csv("/content/BDA NIKHIL PROJECT Book1.csv", inferSchema=True, header=True)

df.printSchema()
df.head(1)
df= df.na.fill(4, subset=['RAM Specifications'])
df.show(1300)
df= df.withColumn("Reviews", when(df["Reviews"] >5.0, df["Reviews"]-4).otherwise(df["Reviews"]))
df.show(1300)



