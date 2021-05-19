from pyspark import SparkContext
import datetime
import csv
import functools
import json
import numpy as np
import sys
import ast
from collections import defaultdict
import statistics
from dateutil import rrule, parser


def main(sc):
    '''
    Transfer our code from the notebook here, however, remember to replace
    the file paths with the ones provided in the problem description.
    '''
    # rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    # rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
    rddPlaces = sc.textFile('/home/amazigh/Desktop/trash_may_2021/Big Data/BDM_HW4_tw0/core-places-nyc.csv')
    rddPattern = sc.textFile('/home/amazigh/Desktop/trash_may_2021/Big Data/BDM_HW4_tw0/weekly-patterns-nyc-2019-2020-sample.csv')

    CAT_CODES = set(['445210', '445110', '722410', '452311', '722513', '445120', '446110', '445299', '722515', '311811', '722511', '445230', '446191', '445291', '445220', '452210', '445292'])
    CAT_GROUP = {"452210": 0 , "452311":0 , "445120": 1, "722410": 2, "722511": 3, "722513": 4, "446110":5,"446191":5, "311811": 6,"722515":6 , "445210": 7, "445220":7, "445230":7, "445291":7, "445292":7, "445299":7, "445110":8 }


    def filterPOIs(_, lines):
        for line in lines:
            line =line.split(",")
            if line[9] in CAT_CODES:
                yield (line[0], CAT_GROUP[line[9]])
            
    rddD = rddPlaces.mapPartitionsWithIndex(filterPOIs).cache()

    storeGroup = dict(rddD.collect())

    groupCount = rddD.map(lambda line: line[1])\
        .map(lambda x : (x,1))\
        .reduceByKey(lambda x,y: x+y)\
        .sortByKey() \
        .map(lambda x: x[1]) \
        .collect()



    def extractVisits(storeGroup, _,lines):
        d= datetime.date(2019, 1,1)
        for idx, line in enumerate(lines):
            line =next(csv.reader([line]))
            if idx>0 and line[0]in storeGroup:
        
                daily=ast.literal_eval(line[16])
                dates = list(rrule.rrule(rrule.DAILY,
                                    dtstart=parser.parse(line[12]),
                                    until=parser.parse(line[13])))
                dates = [(i.date()-d).days for i in dates]
                z= list(zip(dates, daily))
                li=[i for i in z ]
        
                for i in li:
                    if i[0]>=0:
                        yield (storeGroup[line[0]],i[0]), i[1]
            
    rddF = rddPattern \
        .mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))

    def computeStats(groupCount, _, records):

        for i in records:
            key,value = i[0] , i[1]
            sorted_list = sorted(value)
            lenght = groupCount[key[0]] - len(sorted_list)
            listofzeros = [0]* lenght
            new_sorted_list = listofzeros + sorted_list
            str_dev =statistics.stdev(new_sorted_list)
            my_median=statistics.median(new_sorted_list)

            if my_median- str_dev >0:
                low=my_median- str_dev
            else:
                low = 0
            high = my_median + str_dev
            my_date= datetime.datetime(2019, 1, 1) + datetime.timedelta(key[1])
            my_date=my_date.isoformat()[:10]
            year=my_date[:4]
            my_date=my_date.replace(my_date[:4], "2020")
            my_tuple= year,my_date, my_median,low, high 

            yield key[0],','.join(str(element) for element in my_tuple)

    

    rddH = rddF.groupByKey() \
            .mapPartitionsWithIndex(functools.partial(computeStats, groupCount))

    rddJ = rddH.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()

    my_list=['big_box_grocers', 'convenience_stores', 'drinking_places', 'full_service_restaurants',
        'limited_service_restaurants',  'pharmacies_and_drug_stores', 'snack_and_retail_bakeries',
        'specialty_food_stores','supermarkets_except_convenience_stores']


    OUTPUT_PREFIX = sys.argv[1]

    for index, value in enumerate(my_list):
        rddJ.filter(lambda x: x[0]==index or x[0]==-1).values() \
            .saveAsTextFile(f'{OUTPUT_PREFIX}/{value}')
  
 
if __name__=='__main__':
    sc = SparkContext()
    main(sc)