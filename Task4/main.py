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


def get_frequent_items(items: RDD, minimum: int = 100) -> RDD:
    item_pairs = items.map(lambda x: (x, 1))
    item_counts = item_pairs.reduceByKey(lambda x, y: x + y)
    return item_counts.filter(lambda x: x[1] >= minimum)


def k_tuples(items: [], k: int) -> {}:
    tuples = {}
    for c in itertools.combinations(sorted(items), k):
        tuples[c] = 0
    return tuples


def get_frequent_tuples(sessions: RDD, k: int, frequent_items: [], minimum: int = 100) -> {}:
    tuples = k_tuples(frequent_items, k)
    for session in sessions.toLocalIterator():
        session_subset = sorted([item for item in session if item in frequent_items])
        for item in itertools.combinations(session_subset, k):
            if item in tuples:
                tuples[item] += 1
    # noinspection PyTypeChecker
    return dict(filter(lambda x: x[1] >= minimum, tuples.items()))


def get_confidence_singles(tuples: {}, frequent_items: {}) -> []:
    confidence = []
    for key, value in tuples.items():
        confidence.append(((key[0], key[1]), value / frequent_items[key[0]]))
        confidence.append(((key[1], key[0]), value / frequent_items[key[1]]))
    confidence.sort()
    confidence.sort(key=itemgetter(1), reverse=True)
    return confidence


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


def main():
    sc = SparkContext()
    [print() for _ in range(50)]
    file = sc.textFile('4.txt')

    # extract sessions and items
    sessions = file.map(lambda x: split_line(x))
    items = sessions.flatMap(lambda x: x)

    # get frequent items and their combinations with number of occurrences
    frequent_items = dict(get_frequent_items(items).collect())
    frequent_doubles = get_frequent_tuples(sessions, 2, frequent_items)
    frequent_triples = get_frequent_tuples(sessions, 3, items_from_tuples(frequent_doubles))

    # count confidence levels
    doubles_confidence = get_confidence_singles(frequent_doubles, frequent_items)
    triples_confidence = get_confidence_tuples(frequent_triples, frequent_doubles, 3)

    # print
    for d in doubles_confidence[:5]:
        print(d)
    for t in triples_confidence[:5]:
        print(t)

    sc.stop()


if __name__ == '__main__':
    main()
