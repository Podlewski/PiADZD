from os import system, name

import pyspark
from pyspark.ml import Pipeline
from pyspark.ml.feature import StringIndexer
import pyspark.ml.fpm as mlfpm
import pyspark.mllib.fpm as mllibfpm
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, sum


def clear_output(lame: bool = False):
    if lame:
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


ss = SparkSession.builder.getOrCreate()
data = ss.read.csv('data/dfch.csv', header=True, inferSchema=True)
df = data.toDF(*data.columns)
clear_output(True)


# indexers = [StringIndexer(inputCol=col, outputCol=col + '_idx').fit(df)
#             for col in list(set(df.columns) - {'Date'})]
#             # for col in df.columns]
# dfi = Pipeline(stages=indexers).fit(df).transform(df)
# dfi = dfi.select([col for col in dfi.columns if '_idx' in col])
# dff = dfi.withColumn('features', array(*dfi.columns))

fpm = mlfpm.FPGrowth(itemsCol='features', minSupport=0.1, minConfidence=0.1)
model = fpm.fit(df.withColumn('features', array(*list(set(df.columns) - {'Crime Details', 'Crime Description'}))))
result = model.freqItemsets.collect()
for r in result:
    if len(r['items']) > 1:
        print(r)

# fpm = mllibfpm.FPGrowth.train(df.rdd.map(list).cache(), minSupport=0.2)
