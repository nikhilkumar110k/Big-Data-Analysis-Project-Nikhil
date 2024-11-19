from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import when, col, lit, regexp_replace, split
from pyspark.ml.feature import VectorAssembler, StringIndexer, OneHotEncoder
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.types import DoubleType
from pyspark.ml import Pipeline
import os 

os.environ["PYSPARK_PYTHON"] = "c:/Users/hp pc/AppData/Local/Programs/Python/Python311/python.exe" 
os.environ["PYSPARK_DRIVER_PYTHON"] = "c:/Users/hp pc/AppData/Local/Programs/Python/Python311/python.exe"  
spark = SparkSession.builder.appName("BDA Project").getOrCreate()

df = spark.read.csv("Scrapped_Data_for_allinfo.csv", inferSchema=True, header=True)

df = df.withColumn("Prices", regexp_replace(col("Prices"), ",", "").cast(DoubleType()))
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
    handleInvalid="keep" 
)

brand_encoder = OneHotEncoder(
    inputCol="BrandIndex", 
    outputCol="BrandVec",
    handleInvalid="keep" 
)

feature_cols = ["Prices", "RAM Specifications", "Storage Specifications", "Price_Category"]


assembler = VectorAssembler(
    inputCols=feature_cols + ["BrandVec"],
    outputCol="features",
    handleInvalid="keep"  
)

rfr = RandomForestRegressor(
    featuresCol="features", 
    labelCol="Reviews",
    numTrees=100,
    maxDepth=10
)

pipeline = Pipeline(stages=[
    brand_indexer,
    brand_encoder,
    assembler,
    rfr
])

param_grid = ParamGridBuilder() \
    .addGrid(rfr.numTrees, [50, 100]) \
    .addGrid(rfr.maxDepth, [5, 10]) \
    .build()

evaluator = RegressionEvaluator(
    labelCol="Reviews", 
    predictionCol="prediction", 
    metricName="rmse"
)

crossval = CrossValidator(
    estimator=pipeline,
    estimatorParamMaps=param_grid,
    evaluator=evaluator,
    numFolds=2,  
    parallelism=2  
)

print("Training model...")
cv_model = crossval.fit(train_data)
cv_model.bestModel.save("C:/Users/hp pc/OneDrive/Desktop/Big-Data-Analysis-Project-Nikhil/cv_model/model")

print("Making predictions...")
predictions = cv_model.transform(test_data)

rmse = evaluator.evaluate(predictions)
r2 = RegressionEvaluator(
    labelCol="Reviews", 
    predictionCol="prediction", 
    metricName="r2"
).evaluate(predictions)

print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")
print(f"R^2 on test data = {r2}")

if hasattr(cv_model.bestModel.stages[-1], 'featureImportances'):
    feature_importance = cv_model.bestModel.stages[-1].featureImportances
    print("\nFeature Importances:")
    for i, importance in enumerate(feature_importance):
        print(f"Feature {i}: {importance}")

def predict_review(price, ram_spec, storage_spec, brand, pipeline_model):
    input_data = spark.createDataFrame([(
        "Sample Phone", 
        price,          
        0.0,         
        ram_spec,       
        storage_spec, 
        brand,          
        1 if price >= 5000 and price < 20000 else (0 if price < 5000 else 2) 
    )], ["Mobile Name", "Prices", "Reviews", "RAM Specifications", 
         "Storage Specifications", "Brand", "Price_Category"])
    
    try:
        prediction = pipeline_model.transform(input_data)
        return prediction.select("prediction").collect()[0][0]
    except Exception as e:
        print(f"Error making prediction: {str(e)}")
        return None

print("\nMaking sample prediction...")
sample_prediction = predict_review(
    price=15000,
    ram_spec=6,
    storage_spec=128,
    brand="Samsung",
    pipeline_model=cv_model.bestModel
)
if sample_prediction is not None:
    print(f"Predicted review score: {sample_prediction:.2f}")