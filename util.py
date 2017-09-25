from pyspark.sql import SQLContext
from datetime import *
import time as t
def measure_time(query, spark, query_name, result_path):
    f = open(result_path, 'a')
    start=t.time()

    query_result = spark.sql(query)
    f.write(str(query_result.head(1)))

    end=t.time()
    f.write("\n\n%s result time: %.3f seconds\n" % (query_name, (end-start)))
    f.close()
    return None


def test():
    f = open("aa.txt", 'a')
    f.write("%s hi %.3f\n" % ("kkkk", 3.293483))
    f.close()

if __name__ == "__main__":
    test()
    test()
