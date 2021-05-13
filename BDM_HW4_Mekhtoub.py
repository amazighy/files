
from pyspark import SparkContext
from numpy import median, std
import functools
import datetime
import json
import sys
import csv
import ast

 
def main(sc):

    categories=    {
          "big_box_grocers": 0,
          "convenience_stores": 1,
          "drinking_places": 2,
          "full_service_restaurants": 3,
          "limited_service_restaurants": 4,
          "pharmacies_and_drug_stores": 5,
          "snack_and_bakeries":6,
          "specialty_food_stores": 7,
          "supermarkets_except_convenience_stores": 8,
      }
      
    CAT_CODES = set(['445210', '445110', '722410', '452311', '722513', '445120', '446110', '445299', '722515', '311811', '722511', '445230', '446191', '445291', '445220', '452210', '445292'])
    CAT_GROUP = {'452210': 0, '452311': 0, '445120': 1, '722410': 2, '722511': 3, '722513': 4, '446110': 5, '446191': 5, '722515': 6, '311811': 6, '445210': 7, '445299': 7, '445230': 7, '445291': 7, '445220': 7, '445292': 7, '445110': 8}
    
   
    rddPlaces = sc.textFile('/data/share/bdm/core-places-nyc.csv')
    rddPattern = sc.textFile('/data/share/bdm/weekly-patterns-nyc-2019-2020/*')
  

    def filterPOIs(_, lines):
        for line in lines:
            line =line.split(",")
            if line[9] in CAT_CODES:
                yield (line[0], CAT_GROUP[line[9]])


    def extractVisits(storeGroup, _, lines):
        iterline = iter(lines)
        next(iterline)
        for line in iterline:
            line =next(csv.reader([line]))
            if line[0] in storeGroup:
                daily = ast.literal_eval(line[16])
                start_date = datetime.date(int(line[12][:4]), int(line[12][5:7]), int(line[12][8:10]))
                my_date= datetime.date(2019, 1, 1)
                dates = [((start_date + datetime.timedelta(days=day))-my_date).days for day in range(7) ]

                for ixd, date in enumerate(dates):
                    if date>=0:
                        yield (storeGroup[line[0]],date), daily[ixd]


    def getDate(day_num):
        day_num=str(day_num+1)
        day_num.rjust(3 + len(day_num), '0')
        strt_date = datetime.date(2019, 1, 1)
        res_date = strt_date + datetime.timedelta(days=int(day_num) - 1)
        res = res_date.strftime("%Y-%m-%d")
        return res[:4], res.replace(res[:4], '2020')


    def toCSVLine(data):
        return ','.join(str(d) for d in data)


    def computeStats(groupCount, _, records):
        for record in records:
            GC_len=groupCount[record[0][0]]
            visits=sorted(record[1])
            visits_Len= len(visits)
            dif=GC_len-visits_Len
            adj_vis=[0] * dif + visits
            med = median(adj_vis)
            sted = std(adj_vis)
            low = med - sted
            low = low if low > 0 else 0
            high = med + sted
            theYear, theDate =getDate(record[0][1])
            my_string = theYear, theDate, int(med), int(low), int(high)
            yield record[0][0], toCSVLine(my_string)


    rddD = rddPlaces.mapPartitionsWithIndex(filterPOIs).cache()
    groupCount = rddD.map(lambda x:(x[1],x[0]))\
                .combineByKey(lambda v: [v], lambda x, y: x+[y], lambda x, y: x+y)\
                .sortBy(lambda x: x)\
                .map(lambda x:len(x[1]))\
                .collect()
    storeGroup = dict(rddD.collect())
    rddG = rddPattern.mapPartitionsWithIndex(functools.partial(extractVisits, storeGroup))
    rddH = rddG.groupByKey().mapPartitionsWithIndex(functools.partial(computeStats, groupCount))
    rddJ = rddH.sortBy(lambda x: x[1][:15])
    header = sc.parallelize([(-1, 'year,date,median,low,high')]).coalesce(1)
    rddJ = (header + rddJ).coalesce(10).cache()

    
    OUTPUT_PREFIX = sys.argv[1]
    for key, value in categories.items():
        rddJ.filter(lambda x: x[0]==value or x[0]==-1).values() \
            .saveAsTextFile(f'{OUTPUT_PREFIX}/{key}')
    
 
if __name__=='__main__':
    sc = SparkContext()
    main(sc)


    