import itertools as it
from pyspark import SparkContext
import sys


def split_line(line):
    user, friends = line.split('\t')
    return (user, friends.split(','))

def get_mutal_friends(lines):
    friends = lines.flatMap(lambda line: [((line[0], friend), float('inf')) for friend in line[1]])
    possible_friends = lines.flatMap(lambda line: [(pair, 1) for pair in it.permutations(line[1], 2)])

    pairs = friends.union(possible_friends)
    pairs = pairs.reduceByKey(lambda x, y: x + y) \
                 .filter(lambda val: val[1] != float('inf'))
    return pairs

def get_empty_pairs(line):
    return (int(line[0]),())

def transform_pair(line):
    return (int(line[0][0]), (int(line[0][1]), line[1]))

def get_top_ten_recomendations(line):
    return sorted(line, key = lambda x: (-x[1], x[0]))[:10]

def parse_result(line):
    user, recs = line[0], line[1]
    rec_list = [str(rec[0]) for rec in recs]
    return f'{user}\t{",".join(rec_list)}' 


sc = SparkContext()
file = sc.textFile(sys.argv[1])

lines = file.map(lambda line: split_line(line))

pairs = get_mutal_friends(lines)
empty_pairs = lines.map(get_empty_pairs)

result = pairs.map(transform_pair)\
              .groupByKey() \
              .union(empty_pairs) \
              .reduceByKey(lambda x,y: x) \
              .mapValues(get_top_ten_recomendations) \
              .sortByKey()

parsed_result = result.map(parse_result).collect()
with open('result.txt', 'w') as outfile:
    outfile.write('\n'.join(parsed_result))

sc.stop()
