from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import regexp_replace


df= spark.read.csv("/content/Scrapped_Data_for_allinfo.csv", inferSchema=True, header=True)

df=df.withColumn("Prices",(regexp_replace(col("Prices"), ",", "").cast(DoubleType())))
df.dropDuplicates(["Mobile Name", "Prices", "Reviews", "RAM Specifications", "Storage Specifications"])
df.printSchema()
df.head(1)

df= df.na.fill(4, subset=['RAM Specifications'])
df.show(1300)
df= df.withColumn("Reviews", when(df["Reviews"] >5.0, df["Reviews"]-4).otherwise(df["Reviews"]))
df.show(1300)



df0_10000 = df.filter((col("Prices") < 10000))
df0_10000.dropDuplicates(["Mobile Name", "Prices", "Reviews", "RAM Specifications", "Storage Specifications"])
df0_10000.count()

df10000_20000 = df.filter((col("Prices") >= 10000) & (col("Prices") < 20000))
df10000_20000.count()