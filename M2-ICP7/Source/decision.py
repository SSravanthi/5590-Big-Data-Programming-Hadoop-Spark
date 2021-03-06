from pyspark.sql import SparkSession
from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.mllib.evaluation import MulticlassMetrics
from pyspark.sql.functions import col
import sys
import os
os.environ["SPARK_HOME"] = "C:\\spark-2.4.3-bin-hadoop2.7\\"
os.environ["HADOOP_HOME"]="C:\\winutils"
# Create spark session
spark = SparkSession.builder.appName("ICP 14").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Define input path
#input_path = "/home/yong/Desktop/CS5590_Spark/ICP/14/"

# Load data and select feature and label columns
data = spark.read.format("csv").option("header", True).option("inferSchema", True).option("delimiter", ",").load("C:\\Users\\Sravanthi Somalaraju\\Documents\\Bigdata ICPs\\Module2\\ICP7\\ScalaMachineLearning\\adult.csv")
data = data.withColumnRenamed("age", "label").select("label", col("educational-num").alias("educational-num"), col("hours-per-week").alias("hours-per-week"))
data = data.select(data.label.cast("double"), "educational-num", "hours-per-week")

# Create vector assembler for feature column
assembler = VectorAssembler(inputCols=data.columns[1:], outputCol="features")
data = assembler.transform(data)

# Split data into training and test data set
training, test = data.select("label", "features").randomSplit([0.85, 0.15])

# Create Decision tree model and fit the model with training dataset
dt = DecisionTreeClassifier()
model = dt.fit(training)

# Generate prediction from test dataset
predictions = model.transform(test)

# Evuluate the accuracy of the model
evaluator = MulticlassClassificationEvaluator()
accuracy = evaluator.evaluate(predictions)
# Show model accuracy
print("Accuracy:", accuracy)

# Report
predictionAndLabels = predictions.select("label", "prediction").rdd
metrics = MulticlassMetrics(predictionAndLabels)
print("Confusion Matrix:", metrics.confusionMatrix())
print("Precision:", metrics.precision())
print("Recall:", metrics.recall())
print("F-measure:", metrics.fMeasure())