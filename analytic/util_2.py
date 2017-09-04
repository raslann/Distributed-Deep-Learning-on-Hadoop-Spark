from pyspark import SparkContext, SparkConf
from pyspark.storagelevel import StorageLevel
from pyspark.sql import HiveContext
from pyspark.sql.types import IntegerType, StructType, StructField
from pyspark.mllib.linalg import Vectors, SparseVector
import time
import math

def init_spark(verbose_logging=False, show_progress=False):
    if not show_progress:
        SparkContext.setSystemProperty('spark.ui.showConsoleProgress', 'false')
    conf = (SparkConf()
	       .set("spark.executor.memory", "3g"))
    sc = SparkContext(conf=conf)
    sqlContext = HiveContext(sc)
    if verbose_logging:
        sc.setLogLevel(
                'INFO' if isinstance(verbose_logging, bool)
                else verbose_logging
                )
    return sc, sqlContext

def time_since(since):
    now = time.time()
    s = now - since
    m = math.floor(s / 60)
    s -= m * 60
    return '%dm %ds' % (m, s)

start = time.time()

