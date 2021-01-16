from os import system, name

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.linalg import DenseVector
from pyspark.sql import SparkSession, types
from pyspark.ml.feature import VectorAssembler, StringIndexer
from pyspark.ml.fpm import FPGrowth
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType


def clear_output(lame: bool = False):
    if lame:
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


def split_line(line):
    items = line.split(',')
    return items[:1], (items[1:])


ss = SparkSession.builder.getOrCreate()
clear_output(True)

data = ss.read.csv('data/dfny.csv', header=True, inferSchema=True)
df = data.toDF(*data.columns)
clear_output(True)


indexers = [StringIndexer(inputCol=col, outputCol=col + '_idx').fit(df)
            for col in list(set(df.columns) - {'Date'})]
            # for col in df.columns]
dfi = Pipeline(stages=indexers).fit(df).transform(df)
dfi = dfi.select([col for col in dfi.columns if '_idx' in col])
dff = VectorAssembler(inputCols=dfi.columns, outputCol='features').transform(dfi)

aa = udf(lambda x: x, types.ArrayType(types.DoubleType()))
dff = dff.withColumn('features_', aa(dff.features))

dff.show(5)
quit()

[print(x) for x in dff.select('features').take(20)]

result = FPGrowth(itemsCol='features', minSupport=0.2, minConfidence=0.5).fit(dff)

# print(result.take(5))
