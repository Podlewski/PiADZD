from os import name, system
from sys import argv
from timeit import default_timer

from pyspark.sql.functions import array
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.udf import UserDefinedFunction


def clear_output() -> None:
    if len(argv) > 1 and argv[1] == 'True':
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


def load_data() -> ():
    # SETUP #######################################################################
    ss = SparkSession.builder.getOrCreate()
    a__prefix = UserDefinedFunction(lambda x: 'a_' + x, StringType())
    c__prefix = UserDefinedFunction(lambda x: 'c_' + x, StringType())
    l__prefix = UserDefinedFunction(lambda x: 'l_' + x, StringType())
    r__prefix = UserDefinedFunction(lambda x: 'r_' + x, StringType())
    s__prefix = UserDefinedFunction(lambda x: 's_' + x, StringType())
    s_prefix = UserDefinedFunction(lambda x: 's' + x, StringType())
    v_prefix = UserDefinedFunction(lambda x: 'v' + x, StringType())
    timer_l = default_timer()

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

    return dfny, dfch, dfla, timer_l
