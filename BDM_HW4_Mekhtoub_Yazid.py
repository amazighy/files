

import csv
from dateutil import rrule, parser
import ast
from statistics import median
import time
from pyspark import SparkContext
import pandas as pd
import os
import sys


categories = {
    "Big Box GrocerGrocerss": ["452210", "452311"],
    "Convenience Stores": ["445120"],
    "Drinking Places": ["722410"],
    "Full-Service Restaurants": ["722511"],
    "Limited-Service Restaurants": ["722513"],
    "Pharmacies and Drug Stores": ["446110", "446191"],
    "Snack and Bakeries": ["311811", "722515"],
    "Specialty Food Stores": ["445210", "445220", "445230", "445291", "445292",  "445299"],
    "Supermarkets (except Convenience Stores)": ["445110"],
}


def convert_dates(x):
    daily = ast.literal_eval(x[3])
    dates = list(rrule.rrule(rrule.DAILY,
                             dtstart=parser.parse(x[1]),
                             until=parser.parse(x[2])))
    dates = [(i.year, i) for i in dates]
    return list(zip(dates[:-1], daily))


def medianMinMax(x):
    return x[0][0], str(x[0][1])[:10], median(x[1]), min(x[1]), max(x[1])


def toCSVLine(data):
    return ','.join(str(d) for d in data)


def main(sc):
    data = sc.textFile(
        'hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    header = data.first()
    header = sc.parallelize([header])
    data = data.subtract(header)
    head = "year,date,median,min,max"
    head = sc.parallelize([head])

    for i in list(categories.keys()):
        restaurants = set(sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')
                          .map(lambda x: x.split(','))
                          .map(lambda x: (x[1], x[9], x[13]))
                          .filter(lambda x: (x[1] in categories[i]))
                          .map(lambda x: x[0])
                          .collect())

        head.union(
            data.map(lambda x: next(csv.reader([x])))
            .filter(lambda x: x[1] in restaurants)
            .map(lambda x: (x[1], x[12][:10], x[13][:10], x[16]))
            .map(convert_dates)
            .flatMap(lambda x: x)
            .combineByKey(lambda v: [v], lambda x, y: x+[y], lambda x, y: x+y)
            .map(medianMinMax)
            .map(toCSVLine)
        ).saveAsTextFile('output/'+i)


if __name__ == "__main__":
    sc = SparkContext()
    main(sc)
