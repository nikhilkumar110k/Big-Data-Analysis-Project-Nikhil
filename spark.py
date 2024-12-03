from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col, regexp_replace, split
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml import Pipeline
import xgboost as xgb
import pandas as pd
import numpy as np

spark = SparkSession.builder.appName("BDA Project").getOrCreate()

df = spark.read.csv(r"D:\Big-Data-Analysis-Project-Nikhil\Scrapped_Data_for_allinfo.csv", inferSchema=True, header=True)

df = df.withColumn("Prices", regexp_replace(col("Prices"), ",", "").cast("double"))
df = df.na.fill(4, subset=["RAM Specifications"])
df = df.withColumn("Reviews", when(df["Reviews"] > 5.0, df["Reviews"] - 4).otherwise(df["Reviews"]))
df = df.dropDuplicates(["Mobile Name", "Prices", "Reviews", "RAM Specifications", "Storage Specifications"])

df = df.filter((col("Prices") > 100) & (col("Prices") < 50000))
df = df.filter((col("Reviews") >= 0) & (col("Reviews") <= 10))

df = df.withColumn("Brand", split(col("Mobile Name"), " ").getItem(0))

df = df.withColumn(
    "Price_Category",
    when(df["Prices"] < 5000, 0)
    .when((df["Prices"] >= 5000) & (df["Prices"] < 20000), 1)
    .otherwise(2)
)

df0_10000 = df.filter(col("Prices") < 10000)
train_data, test_data = df0_10000.randomSplit([0.7, 0.3], seed=42)

brand_indexer = StringIndexer(
    inputCol="Brand",
    outputCol="BrandIndex",
    handleInvalid="skip"  
)

brand_encoder = OneHotEncoder(
    inputCol="BrandIndex",
    outputCol="BrandVec",
    dropLast=True  
)

feature_cols = ["Prices", "RAM Specifications", "Storage Specifications", "Price_Category"]

assembler = VectorAssembler(
    inputCols=feature_cols + ["BrandVec"],
    outputCol="features",
    handleInvalid="skip"  
)

pipeline = Pipeline(stages=[
    brand_indexer,
    brand_encoder,
    assembler
])

model = pipeline.fit(train_data)

train_transformed = model.transform(train_data)
test_transformed = model.transform(test_data)

train_pd = train_transformed.select("features", "Reviews").toPandas()
test_pd = test_transformed.select("features", "Reviews").toPandas()

X_train = np.array([x.toArray() for x in train_pd["features"]])
y_train = train_pd["Reviews"].values
X_test = np.array([x.toArray() for x in test_pd["features"]])
y_test = test_pd["Reviews"].values

dtrain = xgb.DMatrix(X_train, label=y_train)
dtest = xgb.DMatrix(X_test, label=y_test)

params = {
    'objective': 'reg:squarederror',
    'max_depth': 6,
    'eta': 0.1,
    'eval_metric': 'rmse'
}

xgb_model = xgb.train(params, dtrain, num_boost_round=100)

predictions = xgb_model.predict(dtest)

rmse = np.sqrt(((predictions - y_test) ** 2).mean())
print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

def predict_review(price, ram_spec, storage_spec, brand, model):
    input_data = spark.createDataFrame([(
        "Sample Phone",  
        price,         
        0.0,             
        ram_spec,        
        storage_spec,    
        brand,           
        1 if price >= 5000 and price < 20000 else (0 if price < 5000 else 2)  # Price_Category
    )], ["Mobile Name", "Prices", "Reviews", "RAM Specifications",
         "Storage Specifications", "Brand", "Price_Category"])

    transformed_input = model.transform(input_data)

    input_pd = transformed_input.select("features").toPandas()
    X_input = np.array([x.toArray() for x in input_pd["features"]])

    prediction = xgb_model.predict(xgb.DMatrix(X_input))
    return prediction[0]

print("\nMaking sample prediction...")
sample_prediction = predict_review(
    price=15000,
    ram_spec=6,
    storage_spec=128,
    brand="Samsung",
    model=model
)
print(f"Predicted review score: {sample_prediction:.2f}")
