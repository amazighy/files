from pyspark import SparkContext
import csv
import sys

if __name__ == '__main__':
    sc = SparkContext()
    sc.textFile('pattern_test.csv')\
        .map(lambda x: next(csv.reader([x])))\
        .map(lambda x: (x[1], x[12][:10], x[13][:10], x[16]))\
        .saveAsTextFile(sys.argv[2] if len(sys.argv) > 2 else 'outputs')
