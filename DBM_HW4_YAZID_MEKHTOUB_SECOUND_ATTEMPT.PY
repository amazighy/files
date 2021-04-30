from numpy import std, median
import pyspark
import datetime
import csv
import sys
import ast

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
    daily = ast.literal_eval(x[2])
    start_date = datetime.date(int(x[1][:4]), int(x[1][5:7]), int(x[1][8:10]))
    dates = [(start_date + datetime.timedelta(days=day)).isoformat()
             for day in range(7)]
    return list(zip(dates, daily))
    
def medianMinMax(x):
    med = median(x[1])
    sted = std(x[1])
    low = med - sted
    low = low if low > 0 else 0
    high = med + sted
    return x[0][:4],x[0].replace(x[0][:4], '2020'),int(med), int(low), int(high)

def toCSVLine(data):
    return ','.join(str(d) for d in data)

def main(sc):
    head = "year,date,median,low,high"
    head = sc.parallelize([head])

    for i in list(categories.keys()):
      restaurants = set(sc.textFile('hdfs:///data/share/bdm/core-places-nyc.csv')
                      .map(lambda x: x.split(','))
                      .map(lambda x: (x[1], x[9], x[13]))
                      .filter(lambda x: (x[1] in categories[i]))
                      .map(lambda x: x[0])
                      .collect())
      head.union(
          sc.textFile('hdfs:///data/share/bdm/weekly-patterns-nyc-2019-2020/*') 
          .map(lambda x: next(csv.reader([x]))) 
          .filter(lambda x: x[1] in restaurants) 
          .map(lambda x: (x[1], x[12][:10], x[16]))
          .flatMap(convert_dates)
          .combineByKey(lambda v: [v], lambda x, y: x+[y], lambda x, y: x+y)
          .map(medianMinMax)
          .map(toCSVLine)
          .filter(lambda x: x[:4] != '2018')
          .sortBy(lambda x: x)
      ).saveAsTextFile(sys.argv[-1]+'/'+i.replace(" ", "_") if len(sys.argv) >= 2 else 'output/'+i)



if __name__ == "__main__":
  sc = pyspark.SparkContext()
  main(sc)