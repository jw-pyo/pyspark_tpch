from pyspark.sql import SQLContext
from datetime import *
import time as t
def measure_time(query, spark, query_name):
    start=t.time()

    query_result = spark.sql(query)
    query_result.show()

    end=t.time()
    print(query_name+" result time: %.3f seconds\n" % (end-start))
    return None

def boolify(s):
    if s == 'True':
        return True
    elif s == 'False':
        return False
    raise ValueError("Not boolean")

def autoConvert(s):
    for fn in (boolify, int, float, date):
        try:
            return fn(s)
        except ValueError:
            #print("?\n")
            pass
    return s


if __name__ == "__main__":

    print(type(autoConvert('2343')))
    print(type(autoConvert('234.3')))
    #print(type(autoConvert('dvd')))
    #print(type(autoConvert('2017-01-01')))
    print(type('2015-01-01'))
