from contextlib import redirect_stdout
from os import system, name
from sys import argv
from typing import List

from pyspark.ml.fpm import FPGrowth
from pyspark.sql import SparkSession
from pyspark.sql.functions import array, col
from pyspark.sql.types import Row, StringType, DoubleType, ArrayType
from pyspark.sql.udf import UserDefinedFunction


def clear_output() -> None:
    if len(argv) > 1 and argv[1] == 'True':
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


def print_result(result: List[Row], source_shape: (int, int)) -> None:
    max_tuple_size = max([len(r['items']) for r in result])
    for r in result:
        if len(r['items']) > 1 or max_tuple_size < 2:
            support = r['freq'] / source_shape[0]
            print(f'{list_to_string(r["items"])}; supp={support}')


def printer() -> None:
    print('\n#################################### NEW YORK ###################################')
    resny.associationRules.orderBy('confidence', ascending=False).show(resny.associationRules.count(), False)
    resny.freqItemsets.withColumn(
        'support', ny_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resny.freqItemsets.count(), False)
    print('\n#################################### CHICAGO ####################################')
    resch.associationRules.orderBy('confidence', ascending=False).show(resch.associationRules.count(), False)
    resch.freqItemsets.withColumn(
        'support', ch_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resch.freqItemsets.count(), False)
    print('\n################################## LOS ANGELES ##################################')
    resla.associationRules.orderBy('confidence', ascending=False).show(resla.associationRules.count(), False)
    resla.freqItemsets.withColumn(
        'support', la_supp(col('freq').cast(DoubleType()), col('items').cast(ArrayType(StringType())))
    ).orderBy('freq', ascending=False).show(resla.freqItemsets.count(), False)


def list_to_string(list_: list) -> str:
    return f'[{", ".join(x for x in list_)}]'


# SETUP #######################################################################
ss = SparkSession.builder.getOrCreate()
fpm = FPGrowth(itemsCol='features', minSupport=0.15, minConfidence=0.5)
a__prefix = UserDefinedFunction(lambda x: 'a_' + x, StringType())
c__prefix = UserDefinedFunction(lambda x: 'c_' + x, StringType())
l__prefix = UserDefinedFunction(lambda x: 'l_' + x, StringType())
r__prefix = UserDefinedFunction(lambda x: 'r_' + x, StringType())
s__prefix = UserDefinedFunction(lambda x: 's_' + x, StringType())
s_prefix = UserDefinedFunction(lambda x: 's' + x, StringType())
v_prefix = UserDefinedFunction(lambda x: 'v' + x, StringType())

# NEW YORK ####################################################################
data = ss.read.csv('data/dfny.csv', header=True, inferSchema=True)
dfny = data.toDF(*data.columns)
del data
dfny = dfny.withColumn('Crime Category', c__prefix(dfny['Crime Category']))
dfny = dfny.withColumn('Location Type', l__prefix(dfny['Location Type']))
for c in ['Suspect Age Group', 'Victim Age Group']:
    dfny = dfny.withColumn(c, a__prefix(dfny[c]))
for c in ['Suspect Sex', 'Victim Sex']:
    dfny = dfny.withColumn(c, s__prefix(dfny[c]))
for c in ['Suspect Race', 'Victim Race']:
    dfny = dfny.withColumn(c, r__prefix(dfny[c]))
for c in ['Suspect Age Group', 'Suspect Sex', 'Suspect Race']:
    dfny = dfny.withColumn(c, s_prefix(dfny[c]))
for c in ['Victim Age Group', 'Victim Sex', 'Victim Race']:
    dfny = dfny.withColumn(c, v_prefix(dfny[c]))
dfny = dfny.withColumn(
    'features', array(*list(set(dfny.columns) - {'Date', 'Crime Code', 'Local Crime Code', 'Crime Description'}))
).select('features')

# CHICAGO #####################################################################
data = ss.read.csv('data/dfch.csv', header=True, inferSchema=True)
dfch = data.toDF(*data.columns)
del data
dfch = dfch.withColumn('Crime Category', c__prefix(dfch['Crime Category']))
dfch = dfch.withColumn('Location Type', l__prefix(dfch['Location Type']))
dfch = dfch.withColumn('Arrest', dfch['Arrest'].cast(StringType()))
dfch = dfch.withColumn(
    'features', array(*list(set(dfch.columns) - {'Date', 'Crime Code', 'Local Crime Code', 'Crime Description'}))
).select('features')

# LOS ANGELES #################################################################
data = ss.read.csv('data/dfla.csv', header=True, inferSchema=True)
dfla = data.toDF(*data.columns)
del data
dfla = dfla.withColumn('Crime Category', c__prefix(dfla['Crime Category']))
dfla = dfla.withColumn('Victim Age Group', a__prefix(dfla['Victim Age Group']))
dfla = dfla.withColumn('Victim Sex', s__prefix(dfla['Victim Sex']))
dfla = dfla.withColumn('Victim Race', r__prefix(dfla['Victim Race']))
dfla = dfla.withColumn('Location Type', l__prefix(dfla['Location Type']))
dfla = dfla.withColumn('Arrest', dfla['Arrest'].cast(StringType()))
for c in ['Victim Age Group', 'Victim Sex', 'Victim Race']:
    dfla = dfla.withColumn(c, v_prefix(dfla[c]))
dfla = dfla.withColumn(
    'features', array(*list(set(dfla.columns) - {'Date', 'Crime Code', 'Local Crime Code', 'Crime Description'}))
).select('features')

# CALCULATIONS ################################################################
dfny_len = dfny.count()
dfch_len = dfch.count()
dfla_len = dfla.count()
ny_supp = UserDefinedFunction(lambda x, y: x / dfny_len if len(y) == 1 else '-', StringType())
ch_supp = UserDefinedFunction(lambda x, y: x / dfch_len if len(y) == 1 else '-', StringType())
la_supp = UserDefinedFunction(lambda x, y: x / dfla_len if len(y) == 1 else '-', StringType())

resny = fpm.fit(dfny)
del dfny
resch = fpm.fit(dfch)
del dfch
resla = fpm.fit(dfla)
del dfla

# OUTPUT ######################################################################
clear_output()
with open('result_FPGrowth.txt', 'w') as file:
    with redirect_stdout(file):
        printer()
