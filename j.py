
import csv
from dateutil import rrule, parser
import ast
import time
from pyspark import SparkContext
# import pandas as pd
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


def my_median(sample):
    n = len(sample)
    index = n // 2
    if n % 2:
        return sorted(sample)[index]
    return sum(sorted(sample)[index - 1:index + 1]) / 2


def convert_dates(x):
    daily = ast.literal_eval(x[3])
    dates = list(rrule.rrule(rrule.DAILY,
                             dtstart=parser.parse(x[1]),
                             until=parser.parse(x[2])))
    dates = [(i.year, i) for i in dates]
    return list(zip(dates[:-1], daily))


def medianMinMax(x):
    return x[0][0], x[0][1], my_median(x[1]), min(x[1]), max(x[1])


if __name__ == "__main__":

    sc = SparkContext()
    data = sc.textFile('pattern_test.csv')
    header = data.first()
    header = sc.parallelize([header])
    data = data.subtract(header)

    restaurants = set(sc.textFile('core_poi_ny.csv')
                      .map(lambda x: x.split(','))
                      .map(lambda x: (x[1], x[9], x[13]))
                      .filter(lambda x: (x[1] in categories['Pharmacies and Drug Stores']))
                      .map(lambda x: x[0])
                      .collect())

    data.map(lambda x: next(csv.reader([x])))\
        .filter(lambda x: x[1] in restaurants)\
        .map(lambda x: (x[1], x[12][:10], x[13][:10], x[16]))\
        .map(convert_dates)\
        .flatMap(lambda x: x)\
        .combineByKey(lambda v: [v], lambda x, y: x+[y], lambda x, y: x+y)\
        .map(medianMinMax)\
        .saveAsTextFile('output/drugs')
