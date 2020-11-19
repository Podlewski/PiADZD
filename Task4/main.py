import itertools
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


def print_check(container):
    if isinstance(container, RDD):
        container = container.collect()
    for t in container:
        print(t)
    print(len(container), '\n')


def main():
    sc = SparkContext()
    [print() for _ in range(50)]
    file = sc.textFile('4.txt')

    sessions = file.map(lambda x: split_line(x))
    items = sessions.flatMap(lambda x: x)

    frequent_items = get_frequent_items(items).map(lambda x: x[0])

    frequent_doubles = get_frequent_tuples(sessions, 2,
                                           frequent_items.collect())
    frequent_triples = get_frequent_tuples(sessions, 3,
                                           set([i for d in frequent_doubles for i in d]))

    print_check(frequent_items)
    print_check(frequent_doubles)
    print_check(frequent_triples)

    sc.stop()


if __name__ == '__main__':
    main()
