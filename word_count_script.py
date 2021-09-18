import sys

from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc")
    words = sc.textFile("gs://dataproc-word-count/thor_ragnarok_script.txt").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=True)
    topWords= sc.parallelize(wordCounts.take(20))
    topWords.saveAsTextFile("gs://dataproc-word-count/results_script")