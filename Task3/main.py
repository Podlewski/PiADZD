from pyspark import SparkContext
from statistics import mean
import matplotlib.pyplot as plt
import numpy as np
import math

MAX_ITERATIONS = 20
METRICS = ['euclidean', 'manhattan']
FILES = ['3b.txt', '3c.txt']
NAMES = ['Random centroids', 'Max. dist. centroids']


def split_line(line):
    return [float(x) for x in line.split()]


def euclidean_distance(vector, centroid):
    sum = 0
    for i in range(len(vector)):
        sum += (vector[i]-centroid[i]) ** 2
    return math.sqrt(sum)


def manhattan_distance(vector, centroid):
    sum = 0
    for i in range(len(vector)):
        sum += abs(vector[i]-centroid[i])
    return sum


def euclidean_cost(vector, centroid):
    return euclidean_distance(vector, centroid) ** 2


def manhattan_cost(vector, centroid):
    return manhattan_distance(vector, centroid)


def cost(metric, vector, centroid):
    if metric == 'euclidean':
        return euclidean_cost(vector, centroid)
    else:
        return manhattan_cost(vector, centroid)


def distance(metric, vector, centroid):
    if metric == 'euclidean':
        return euclidean_distance(vector, centroid)
    else:
        return manhattan_distance(vector, centroid)


def find_nearest_centroid(metric, vector, centroids):
    nearest = (0, distance(metric, vector, centroids[0]))
    for index, value in enumerate(centroids):
        dist = distance(metric, vector, value)
        if dist <= nearest[1]:
            nearest = (index, dist)
    return nearest[0], vector


def calculate_mean(vectors):
    return list(map(mean, zip(*vectors)))


def kmeans(file, max_iterations, vectors, metric):
    centroids = np.loadtxt(file)
    cost_values = []

    for _ in range(max_iterations):
        pairs = vectors.map(
            lambda x: find_nearest_centroid(metric, x, centroids))
        cost_val = pairs.map(lambda x: cost(
            metric, x[1], centroids[x[0]])).sum()
        cost_values.append(cost_val)
        new_centroids = pairs.groupByKey().mapValues(calculate_mean)
        centroids = new_centroids.map(lambda x: x[1]).collect()

    return cost_values


def main():
    sc = SparkContext()
    data = sc.textFile('3a.txt')
    vectors = data.map(split_line)
    cost_values_plot = []

    for metric in METRICS:
        for file in FILES:
            cost_values = kmeans(file, MAX_ITERATIONS, vectors, metric)
            cost_values_plot.append(cost_values)
            print('File:', file)
            print('Metric:', metric)

            for i, val in enumerate(cost_values):
                print(f'{i:2d}:  {val:14.2f}')

            cost_change = (cost_values[0]-cost_values[9])/cost_values[0] 
            print(f'Percent cost change: {cost_change*100:04.2f}%')

    sc.stop()

    euclidean_plot_range = (min(cost_values_plot[0]+cost_values_plot[1])*0.8,
                            max(cost_values_plot[0]+cost_values_plot[1])*1.1)
    manhattan_plot_range = (min(cost_values_plot[2]+cost_values_plot[3])*0.8,
                            max(cost_values_plot[2]+cost_values_plot[3])*1.1)

    _, axs = plt.subplots(figsize=(6,8), nrows=2, ncols=2)

    axs[0, 0].set_title(NAMES[0], y=1.05, size='large')
    axs[0, 1].set_title(NAMES[1], y=1.05, size='large')
    axs[0, 0].plot(cost_values_plot[0], 'limegreen')
    axs[0, 1].plot(cost_values_plot[1], 'darkgreen')
    axs[0, 0].set_ylim(euclidean_plot_range)
    axs[0, 1].set_ylim(euclidean_plot_range)
    axs[0, 0].annotate('Euclidean metric', xy=(0, 0.5), rotation=90,
                       xytext=(-axs[1, 0].yaxis.labelpad - 9.5, 0),
                       xycoords=axs[0, 0].yaxis.label, size='large',
                       textcoords='offset points', ha='right', va='center')
    axs[1, 0].plot(cost_values_plot[2], 'royalblue')
    axs[1, 1].plot(cost_values_plot[3], 'mediumblue')
    axs[1, 0].set_ylim(manhattan_plot_range)
    axs[1, 1].set_ylim(manhattan_plot_range)
    axs[1, 0].annotate('Manhattan metric', xy=(0, 0.5), rotation=90,
                       xytext=(-axs[1, 0].yaxis.labelpad, 0),
                       xycoords=axs[1, 0].yaxis.label, size='large',
                       textcoords='offset points', ha='right', va='center')

    for ax in axs.flat:
        ax.set(xlabel='Iterations', ylabel='Cost')
        ax.label_outer()

    plt.tight_layout()
    plt.show()
    # plt.savefig('task3_chart', dpi=600)

if __name__ == "__main__":
    main()
