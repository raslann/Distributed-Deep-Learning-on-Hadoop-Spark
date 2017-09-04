from util import *
import numpy as NP
import numpy.random as RNG
import copy
from pyspark import SparkContext, SparkConf
from pyspark.mllib.linalg import DenseVector, VectorUDT
from pyspark.sql import SQLContext
from pyspark.ml.classification import MultilayerPerceptronClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, ArrayType

sc, sqlContext = init_spark(verbose_logging='INFO', show_progress=False)


def data_frame_from_file(sqlContext, file_name, fraction):
    lines = sc.textFile(file_name).sample(False, fraction)
    parts = lines.map(lambda l: map(lambda s: int(s), l.split(",")))
    samples = parts.map(lambda p: (
        float(p[0]),
        DenseVector(map(lambda el: el / 255.0, p[1:]))
    ))

    fields = [
    StructField("label", DoubleType(), True),
        StructField("features", VectorUDT(), True)
    ]
    schema = StructType(fields)

    data = sqlContext.createDataFrame(samples, schema)
    return data

print("step0")
train = data_frame_from_file(sqlContext, "mnist_train.csv", 1)
test = data_frame_from_file(sqlContext, "mnist_test.csv", 1)
print("step1")

# The following model specification comes from Pyspark ML Package Examples

layers = [28*28, 1024, 10]
trainer = MultilayerPerceptronClassifier(maxIter=1000, layers=layers, blockSize=128, seed=1234)
model = trainer.fit(train)
result = model.transform(test)
predictionAndLabels = result.select("prediction", "label")
evaluator = MulticlassClassificationEvaluator(metricName="precision")
print("Precision: " + str(evaluator.evaluate(predictionAndLabels)))
print("step2")
print("Time Elapsed: ", time_since(start))