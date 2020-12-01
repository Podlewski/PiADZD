import itertools
from operator import itemgetter
from os import system, name
from pyspark import SparkContext
from pyspark.rdd import RDD


def clear_output():
    system('cls' if name == 'nt' else 'clear')


def split_line(line):
    items = line.split(' ')
    while '' in items:
        items.remove('')
    return items


def k_tuples(session, items: [], k: int):
    tuples = [item for item in session if item in items]
    return [(c, 1) for c in itertools.combinations(sorted(tuples), k)]


def get_frequent_tuples(sessions: RDD, k: int, frequent_items: [], minimum: int = 100) -> {}:
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


def print_results(array: []):
    for i in array:
        print(f'{i[0][:-1]} => {i[0][-1]}\t: conf = {i[1]}')


def main():
    sc = SparkContext()
    clear_output()

    sessions = sc.textFile('4.txt').map(lambda x: split_line(x))

    # get frequent items and their combinations with number of occurrences
    frequent_singles = get_frequent_tuples(sessions, 1, None)
    frequent_doubles = get_frequent_tuples(sessions, 2, items_from_tuples(frequent_singles))
    frequent_triples = get_frequent_tuples(sessions, 3, items_from_tuples(frequent_doubles))

    # count confidence levels
    doubles_confidence = get_confidence_tuples(frequent_doubles, frequent_singles, 2)
    triples_confidence = get_confidence_tuples(frequent_triples, frequent_doubles, 3)

    # print
    print('Predecessor => Successor\t: confidence level')
    print_results(doubles_confidence[:5])
    print_results(triples_confidence[:5])

    # save
    with open('result_doubles.txt', 'w') as fout:
        for i in doubles_confidence:
            fout.write(f'{i[0][0]} [{i[0][-1]}] {i[1]}\n')
    with open('result_triples.txt', 'w') as fout:
        for i in triples_confidence:
            fout.write(f'{i[0][0]} {i[0][1]} [{i[0][-1]}] {i[1]}\n')

    sc.stop()


if __name__ == '__main__':
    main()
