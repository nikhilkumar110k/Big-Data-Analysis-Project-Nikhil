from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import when, col, lit
from pyspark.ml.feature import  VectorAssembler
from pyspark.sql.types import DoubleType
from pyspark.sql.functions import regexp_replace
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator


spark= SparkSession.builder.appName("BDA Project").getOrCreate()

df= spark.read.csv("Scrapped_Data_for_allinfo.csv", inferSchema=True, header=True)

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
output0_10000= assembler.transform(df0_10000)
fnl_data0_10000=output0_10000.select('features','Reviews')
fnl_data0_10000.show()
train_fnl_data, test_fnl_data= fnl_data0_10000.randomSplit([0.7,0.3])
fnl_data0_10000.na.drop()
fnl_data0_10000.count()
rfr= RandomForestRegressor(featuresCol='features',labelCol='Reviews')

rfr_model=rfr.fit(train_fnl_data)
predictions= rfr_model.transform(test_fnl_data)
predictions.select("prediction", "Reviews", "features").show(20)
evaluator_rmse = RegressionEvaluator(labelCol="Reviews", predictionCol="prediction", metricName="rmse")
evaluator_r2 = RegressionEvaluator(labelCol="Reviews", predictionCol="prediction", metricName="r2")
rmse = evaluator_rmse.evaluate(predictions)
r2 = evaluator_r2.evaluate(predictions)

print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")
print(f"R^2 on test data = {r2}")

prices_int= float(input("enter price"))
ram_int= float(input("enter RAM"))
storage_int= float(input("enter storage"))

user_input= {'Prices': prices_int, 'RAM Specifications':ram_int,'Storage Specifications':storage_int}
user_input= Row(**user_input)
user_input= spark.createDataFrame([user_input])
user_input.printSchema()

assembler1= VectorAssembler(inputCols=['Prices','RAM Specifications','Storage Specifications'], outputCol='features', handleInvalid='skip')
vectors_input_byuser = assembler1.transform(user_input)

rfr_predictions0_10000= rfr_model.transform(vectors_input_byuser)

rfr_predictions0_10000.select("features", "prediction").show(truncate=False)


