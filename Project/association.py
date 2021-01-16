from contextlib import redirect_stdout
from os import system, name
from sys import argv
from typing import List

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.functions import array
from pyspark.sql.types import Row, StringType
from pyspark.sql.udf import UserDefinedFunction


def clear_output() -> None:
    if len(argv) > 1 and argv[1] == 'True':
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


def print_result(result: List[Row]) -> None:
    max_tuple_size = max([len(r['items']) for r in result])
    for r in result:
        if len(r['items']) > 1 or max_tuple_size < 2:
            print(r)


# SETUP #######################################################################
ss = SparkSession.builder.getOrCreate()
fpm = FPGrowth(itemsCol='features', minSupport=0.2, minConfidence=0.1)
a__prefix = UserDefinedFunction(lambda x: 'a_' + x, StringType())
s__prefix = UserDefinedFunction(lambda x: 's_' + x, StringType())
r__prefix = UserDefinedFunction(lambda x: 'r_' + x, StringType())
v_prefix = UserDefinedFunction(lambda x: 'v' + x, StringType())
s_prefix = UserDefinedFunction(lambda x: 's' + x, StringType())

# NEW YORK ####################################################################
data = ss.read.csv('data/dfny.csv', header=True, inferSchema=True).drop('_c0')
dfny = data.toDF(*data.columns)
del data
for col in ['Suspect Age Group', 'Victim Age Group']:
    dfny = dfny.withColumn(col, a__prefix(dfny[col]))
for col in ['Suspect Sex', 'Victim Sex']:
    dfny = dfny.withColumn(col, s__prefix(dfny[col]))
for col in ['Suspect Race', 'Victim Race']:
    dfny = dfny.withColumn(col, r__prefix(dfny[col]))
for col in ['Suspect Age Group', 'Suspect Sex', 'Suspect Race']:
    dfny = dfny.withColumn(col, s_prefix(dfny[col]))
for col in ['Victim Age Group', 'Victim Sex', 'Victim Race']:
    dfny = dfny.withColumn(col, v_prefix(dfny[col]))
dfny = dfny.withColumn(
    'features', array(*list(set(dfny.columns) - {'Date', 'Crime Description'}))
).select('features')

# CHICAGO #####################################################################
data = ss.read.csv('data/dfch.csv', header=True, inferSchema=True).drop('_c0')
dfch = data.toDF(*data.columns)
del data
dfch = dfch.withColumn(
    'features', array(*list(set(dfch.columns) - {'Date', 'Crime Details', 'Crime Description'}))
).select('features')

# LOS ANGELES #################################################################
data = ss.read.csv('data/dfla.csv', header=True, inferSchema=True).drop('_c0')
dfla = data.toDF(*data.columns)
del data
dfla = dfla.withColumn('Victim Age Group', a__prefix(dfla['Victim Age Group']))
dfla = dfla.withColumn('Victim Sex', s__prefix(dfla['Victim Sex']))
dfla = dfla.withColumn('Victim Race', r__prefix(dfla['Victim Race']))
dfla = dfla.withColumn(
    'features', array(*list(set(dfla.columns) - {'Date', 'Crime Description'}))
).select('features')

# CALCULATIONS ################################################################
resny = fpm.fit(dfny).freqItemsets.collect()
del dfny
resch = fpm.fit(dfch).freqItemsets.collect()
del dfch
resla = fpm.fit(dfla).freqItemsets.collect()
del dfla

# OUTPUT ######################################################################
clear_output()
with open('result_FPGrowth.txt', 'w') as file:
    with redirect_stdout(file):
        print('\n###### NEW YORK #####')
        print_result(resny)
        print('\n###### CHICAGO ######')
        print_result(resch)
        print('\n#### LOS ANGELES ####')
        print_result(resla)

print('\n###### NEW YORK #####')
print_result(resny)
print('\n###### CHICAGO ######')
print_result(resch)
print('\n#### LOS ANGELES ####')
print_result(resla)
