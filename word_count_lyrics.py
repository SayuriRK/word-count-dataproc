import sys
from pyspark import SparkContext, SparkConf
if __name__ == "__main__":
    sc = SparkContext("local","PySpark Exemplo - Desafio Dataproc")
    words = sc.read.json("gs://dataproc-word-count/drake_data.json").flatMap(lambda line: line.split(" "))
    wordCounts = words.map(lambda word: (word, 1)).reduceByKey(lambda a,b:a +b).sortBy(lambda a:a[1], ascending=False)
    topWords= sc.parallelize(wordCounts.take(15))
    topWords.saveAsTextFile("gs://dataproc-word-count/results_lyrics")