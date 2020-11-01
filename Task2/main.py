from pyspark import SparkContext

sc = SparkContext()

data = sc.textFile('2.txt').map(lambda row: row.split())

withFriends = data.filter(lambda x: len(x) > 1).map(
    lambda x: (x[0], x[1].split(",")))
print(withFriends.take(5))

withoutFriends = data.filter(lambda x: len(
    x) == 1).flatMap(lambda x: x)
print(withoutFriends.take(5))
