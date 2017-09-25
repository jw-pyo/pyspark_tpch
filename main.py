#system library
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, Row
from datetime import datetime
import time as t
import logging
import logging.handlers
import subprocess
import shlex
from StringIO import StringIO
#project foler library
from tpch_query import *
from tpch_schema import *
from util import *


#PATH
data_path="/data/tpch_sf100/"
RESULT_PATH="./result_tpch100.txt"
LOG_PATH="./memory_tpch100.log"

#log
logger = logging.getLogger('mylogger')
fileHandler = logging.FileHandler(LOG_PATH)
formatter = logging.Formatter('[%(levelname)s|%(filename)s:%(lineno)s] %(asctime)s > \n%(message)s')
fileHandler.setFormatter(formatter)

logger.addHandler(fileHandler)
logger.setLevel(logging.DEBUG)

logger.info("Before table loading\n\n"+ \
                str(subprocess.check_output(['free', '-g'])))

conf = SparkConf()
#conf.setMaster("spark://kdb-spark:7731")
conf.setAppName("DBhw1")
conf.set("spark.driver.memory", "16g")
conf.set("spark.executor.memory", "300g")
#conf.set("spark.master", "spark://kdb-spark:7077")
conf.set("spark.ui.port", "37041")
#conf.set("spark.sql.shuffle.partitions", "200")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext

#data load and create each dataframe


lineitem = sc.textFile(DATA_PATH+"lineitem.tbl").map(lambda l: l.split('|')) \
        .map(lambda l: [int(l[0]),int(l[1]), int(l[2]), int(l[3]), float(l[4]), float(l[5]), float(l[6]), float(l[7]), str(l[8]), str(l[9]), datetime.strptime(l[10], "%Y-%m-%d"), datetime.strptime(l[11], "%Y-%m-%d"), datetime.strptime(l[12], '%Y-%m-%d'), str(l[13]), str(l[14]), str(l[15])])

part = sc.textFile(DATA_PATH+"part.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]),str(l[1]), str(l[2]), str(l[3]), str(l[4]), int(l[5]), str(l[6]), float(l[7]), str(l[8])])

supplier = sc.textFile(DATA_PATH+"supplier.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), str(l[1]), str(l[2]), int(l[3]), str(l[4]), float(l[5]), str(l[6])])

partsupp = sc.textFile(DATA_PATH+"partsupp.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), int(l[1]), int(l[2]), float(l[3]), str(l[4])])

customer = sc.textFile(DATA_PATH+"customer.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), str(l[1]), str(l[2]), int(l[3]), str(l[4]), float(l[5]), str(l[6]), str(l[7])])

orders = sc.textFile(DATA_PATH+"orders.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), int(l[1]), str(l[2]), float(l[3]), datetime.strptime(l[4], '%Y-%m-%d'), str(l[5]), str(l[6]), int(l[7]), str(l[8])])

nation = sc.textFile(DATA_PATH+"nation.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), str(l[1]), int(l[2]), str(l[3])])

region = sc.textFile(DATA_PATH+"region.tbl").map(lambda l: l.split('|')) \
               .map(lambda l: [int(l[0]), str(l[1]), str(l[2])])
#register table
df_lineitem = spark.createDataFrame(lineitem, schema_lineitem)
df_lineitem.registerTempTable("lineitem")
df_lineitem.cache().count()

df_part = spark.createDataFrame(part, schema_part)
df_part.registerTempTable("part")
df_part.cache().count()

df_supplier = spark.createDataFrame(supplier, schema_supplier)
df_supplier.registerTempTable("supplier")
df_supplier.cache().count()

df_partsupp = spark.createDataFrame(partsupp, schema_partsupp)
df_partsupp.registerTempTable("partsupp")
df_partsupp.cache().count()

df_customer = spark.createDataFrame(customer, schema_customer)
df_customer.registerTempTable("customer")
df_customer.cache().count()

df_orders = spark.createDataFrame(orders, schema_orders)
df_orders.registerTempTable("orders")
df_orders.cache().count()

df_nation = spark.createDataFrame(nation, schema_nation)
df_nation.registerTempTable("nation")
df_nation.cache().count()

df_region = spark.createDataFrame(region, schema_region)
df_region.registerTempTable("region")
df_region.cache().count()

logger.info("After table loading\n\n"+str(subprocess.check_output(['free', '-g'])))
#querying
measure_time(q1, spark, "Q1", RESULT_PATH)
logger.info("After executing Q1\n\n"+str(subprocess.check_output(['free', '-g'])))
measure_time(q3, spark, "Q3", RESULT_PATH)
measure_time(q5, spark, "Q5", RESULT_PATH)
measure_time(q7, spark, "Q7", RESULT_PATH)
measure_time(q16, spark, "Q16", RESULT_PATH)
measure_time(q18, spark, "Q18", RESULT_PATH)
measure_time(q20, spark, "Q20", RESULT_PATH)
measure_time(q22, spark, "Q22", RESULT_PATH)
logger.info("Finish executing all queries\n\n"+str(subprocess.check_output(['free', '-g'])))
