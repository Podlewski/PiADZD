import itertools
from contextlib import redirect_stdout
from operator import itemgetter
from os import system, name
from sys import argv

from pyspark.rdd import RDD
from pyspark.sql import SparkSession
from pyspark.sql.functions import array
from pyspark.sql.types import StringType
from pyspark.sql.udf import UserDefinedFunction


def clear_output() -> None:
    if len(argv) > 1 and argv[1] == 'True':
        [print() for _ in range(50)]
    else:
        system('cls' if name == 'nt' else 'clear')


def k_tuples(session, items: [], k: int):
    tuples = [item for item in session if item in items]
    return [(c, 1) for c in itertools.combinations(sorted(tuples), k)]


def get_frequent_tuples(sessions: RDD, k: int, frequent_items: [], minimum: int = 200) -> {}:
    if k < 2:
        tuples = sessions.flatMap(lambda x: x).map(lambda x: ((x,), 1))
    else:
        tuples = sessions.flatMap(lambda x: k_tuples(x, frequent_items, k))
    tuples = tuples.reduceByKey(lambda x, y: x + y)
    return dict(tuples.filter(lambda x: x[1] >= minimum).collect())


def items_from_tuples(tuples: {}) -> set:
    return set([item for t in tuples for item in t])


def get_confidence_tuples(tuples: {}, frequent_tuples: {}, k: int) -> []:
    confidence = []
    for key, value in tuples.items():
        for c in itertools.combinations(key, k - 1):
            if c in frequent_tuples:
                pred = list(c)
                succ = list(set(key) - set(c))
                confidence.append((tuple(pred + succ), value / frequent_tuples[c]))
    confidence.sort()
    confidence.sort(key=itemgetter(1), reverse=True)
    return confidence


def list_to_string(list_: list) -> str:
    return f'[{", ".join(x for x in list_)}]'


def print_confidence(array_: []):
    print('+--------------------------------------------------+--------------------+--------------------+')
    print('|antecedent                                        |consequent          |confidence          |')
    print('+--------------------------------------------------+--------------------+--------------------+')
    for i in array_:
        print(f'|{list_to_string(i[0][:-1]):<50}|{i[0][-1]:<20}|{i[1]:<20}|')
    print('+--------------------------------------------------+--------------------+--------------------+')


def print_frequency(array_: []):
    print('+--------------------------------------------------+--------------------+--------------------+')
    print('|items                                             |freq                |support             |')
    print('+--------------------------------------------------+--------------------+--------------------+')
    for i in array_:
        print(f'|{list_to_string(i[0]):<50}|{i[1]:<20}|{i[2]:<20}|')
    print('+--------------------------------------------------+--------------------+--------------------+')


def run(rdd_: RDD, support: float = 0.15, confidence: float = 0.5) -> None:
    # get frequent items and their combinations with number of occurrences
    supp = int(support * rdd_.count())
    frequent_singles = get_frequent_tuples(rdd_, 1, None, minimum=supp)
    frequent_doubles = get_frequent_tuples(rdd_, 2, items_from_tuples(frequent_singles), minimum=supp)
    frequent_triples = get_frequent_tuples(rdd_, 3, items_from_tuples(frequent_doubles), minimum=supp)

    # count confidence levels
    doubles_confidence = get_confidence_tuples(frequent_doubles, frequent_singles, 2)
    triples_confidence = get_confidence_tuples(frequent_triples, frequent_doubles, 3)

    # print
    conf = doubles_confidence + triples_confidence
    print_confidence(filter(lambda x: x[1] > confidence, conf))

    frequent_singles.update(frequent_doubles)
    frequent_singles.update(frequent_triples)
    source_size = rdd_.count()
    freq = [(i, freq, "-" if i is list else freq / source_size) for i, freq in frequent_singles.items()]
    print_frequency(sorted(freq, key=lambda x: -x[1]))


# SETUP #######################################################################
ss = SparkSession.builder.getOrCreate()
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

# GO ##########################################################################
clear_output()
with open('result_Apriori.txt', 'w') as file:
    with redirect_stdout(file):
        run(dfny.rdd.flatMap(lambda row: [x for x in row]))
        run(dfch.rdd.flatMap(lambda row: [x for x in row]))
        run(dfla.rdd.flatMap(lambda row: [x for x in row]))
