from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, lit
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import regexp_replace
from pyspark.ml.regression import LinearRegression
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler

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



df10000_40000 = df.filter((col("Prices") >= 10000) & (col("Prices") < 40000))
df10000_40000.dropDuplicates(["Mobile Name", "Prices", "Reviews", "RAM Specifications", "Storage Specifications"])
df10000_40000.count()


train_datafrom0_10000, test_datafrom0_10000= df0_10000.randomSplit([0.7,0.3])

train_datafrom0_10000.describe().show()
test_datafrom0_10000.describe().show()




assembler= VectorAssembler(inputCols=['Prices','Reviews','RAM Specifications','Storage Specifications'], outputCol='features', handleInvalid='skip')
output= assembler.transform(df)
fnl_data=output.select('features','Reviews')
fnl_data.show()
train_fnl_data, test_fnl_data= fnl_data.randomSplit([0.7,0.3])
fnl_data.na.drop()
fnl_data.count()
lr= LinearRegression(labelCol='Reviews')

lr_model=lr.fit(train_fnl_data)
result_accuracy= lr_model.evaluate(test_fnl_data)
result_accuracy.rootMeanSquaredError
result_accuracy.r2
fnl_data.describe().show()








