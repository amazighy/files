# from pyspark import SparkContext
# import sys

# if __name__=='__main__':
#     sc = SparkContext()
#     sc.textFile(sys.argv[1] if len(sys.argv)>1 else 'book.txt') \
#         .flatMap(lambda x: x.split()) \
#         .map(lambda x: (x,1)) \
#         .reduceByKey(lambda x,y: x+y) \
#         .saveAsTextFile(sys.argv[2] if len(sys.argv)>2 else 'output')

from pyspark import SparkContext
import csv


if __name__ == '__main__':
    sc = SparkContext()
    rdd = (sc.textFile('pattern_test.csv')
           .map(lambda x: next(csv.reader([x])))
           .map(lambda x: (x[1], x[12][:10], x[13][:10], x[16]))
           .collect())
    header = ['year', 'date', 'median', 'low']
    with open('myfile.csv', 'w') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(rdd)
