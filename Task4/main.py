from pyspark import SparkContext
from pyspark.rdd import RDD


def split_line(line):
    items = line.split(' ')
    while '' in items:
        items.remove('')
    return items


def get_support(items: RDD, minimum: int = 100) -> RDD:
    item_pairs = items.map(lambda x: (x, 1))
    item_counts = item_pairs.reduceByKey(lambda x, y: x + y)
    return item_counts.filter(lambda x: x[1] > minimum)


def main():
    sc = SparkContext()
    [print() for _ in range(50)]
    file = sc.textFile('4.txt')

    sessions = file.map(lambda x: split_line(x))
    items = sessions.flatMap(lambda x: x)

    frequent_items = get_support(items)

    sc.stop()


if __name__ == '__main__':
    main()
