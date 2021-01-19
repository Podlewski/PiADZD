import itertools
from contextlib import redirect_stdout
from operator import itemgetter
from timeit import default_timer

from pyspark.rdd import RDD

from association_common import clear_output, load_data


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
    print('+--------------------------------------------------+--------------------+--------------------+\n')


def print_frequency(array_: []):
    print('+--------------------------------------------------+--------------------+--------------------+')
    print('|items                                             |freq                |support             |')
    print('+--------------------------------------------------+--------------------+--------------------+')
    for i in array_:
        print(f'|{list_to_string(i[0]):<50}|{i[1]:<20}|{i[2]:<20}|')
    print('+--------------------------------------------------+--------------------+--------------------+\n')


def run(rdd_: RDD, support: float = 0.15, confidence: float = 0.5) -> None:
    # get frequent items and their combinations with number of occurrences
    supp = int(support * rdd_.count())
    frequent_singles = get_frequent_tuples(rdd_, 1, None, minimum=supp)
    frequent_doubles = get_frequent_tuples(rdd_, 2, items_from_tuples(frequent_singles), minimum=supp)
    frequent_triples = get_frequent_tuples(rdd_, 3, items_from_tuples(frequent_doubles), minimum=supp)
    frequent_quadras = get_frequent_tuples(rdd_, 4, items_from_tuples(frequent_triples), minimum=supp)

    # count confidence levels
    doubles_confidence = get_confidence_tuples(frequent_doubles, frequent_singles, 2)
    triples_confidence = get_confidence_tuples(frequent_triples, frequent_doubles, 3)
    quadras_confidence = get_confidence_tuples(frequent_quadras, frequent_triples, 4)

    # print
    conf = doubles_confidence + triples_confidence + quadras_confidence
    print_confidence(sorted(filter(lambda x: x[1] > confidence, conf), key=lambda x: -x[1]))

    frequent_singles.update(frequent_doubles)
    frequent_singles.update(frequent_triples)
    frequent_singles.update(frequent_quadras)
    source_size = rdd_.count()
    freq = [(i, freq, "-" if i is list else freq / source_size) for i, freq in frequent_singles.items()]
    print_frequency(sorted(freq, key=lambda x: -x[1]))


# CALCULATIONS ################################################################
dfny, dfch, dfla, timer_l = load_data()
timer_c = default_timer()

# OUTPUT ######################################################################
clear_output()
with open('result_Apriori.txt', 'w') as file:
    with redirect_stdout(file):
        print('\n#################################### NEW YORK ###################################')
        run(dfny.rdd.flatMap(lambda row: [x for x in row]))
        print('\n#################################### CHICAGO ####################################')
        run(dfch.rdd.flatMap(lambda row: [x for x in row]))
        print('\n################################## LOS ANGELES ##################################')
        run(dfla.rdd.flatMap(lambda row: [x for x in row]))
        print(f'Data loading time:\t{timer_c - timer_l:.2f} s')
        print(f'Computing time:   \t{default_timer() - timer_c:.2f} s')
