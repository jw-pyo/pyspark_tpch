#system library
from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql.types import *
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext, Row
from datetime import datetime
import time as t

#project foler library
from tpch_query import *
from tpch_schema import *
from util import *


conf = (SparkConf().setAppName("DBhw1"))
conf.set("spark.driver.memory", "16g")
conf.set("spark.executor.memory", "250g")
conf.set("spark.ui.port", "33333")
conf.set("spark.executor.cores", "48")
#conf.set("spark.sql.shuffle.partitions", "200")
spark = SparkSession.builder.config(conf=conf).getOrCreate()

sc = spark.sparkContext

#data load and create each dataframe

data_path="/nfs-data/datasets/tpch_sf100/"

lineitem = sc.textFile(data_path+"lineitem.tbl").map(lambda l: l.split('|')) \
        .map(lambda l: [int(l[0]),int(l[1]), int(l[2]), int(l[3]), float(l[4]), float(l[5]), float(l[6]), float(l[7]), str(l[8]), str(l[9]), datetime.strptime(l[10], "%Y-%m-%d"), datetime.strptime(l[11], "%Y-%m-%d"), datetime.strptime(l[12], '%Y-%m-%d'), str(l[13]), str(l[14]), str(l[15])])

part = sc.textFile(data_path+"part.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]),str(l[1]), str(l[2]), str(l[3]), str(l[4]), int(l[5]), str(l[6]), float(l[7]), str(l[8])])

supplier = sc.textFile(data_path+"supplier.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]), str(l[1]), str(l[2]), int(l[3]), str(l[4]), float(l[5]), str(l[6])])

partsupp = sc.textFile(data_path+"partsupp.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]), int(l[1]), int(l[2]), float(l[3]), str(l[4])])

customer = sc.textFile(data_path+"customer.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]), str(l[1]), str(l[2]), int(l[3]), str(l[4]), float(l[5]), str(l[6]), str(l[7])])

orders = sc.textFile(data_path+"orders.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]), int(l[1]), str(l[2]), float(l[3]), datetime.strptime(l[4], '%Y-%m-%d'), str(l[5]), str(l[6]), int(l[7]), str(l[8])])

nation = sc.textFile(data_path+"nation.tbl").map(lambda l: l.split('|')) \
                .map(lambda l: [int(l[0]), str(l[1]), int(l[2]), str(l[3])])

region = sc.textFile(data_path+"region.tbl").map(lambda l: l.split('|')) \
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

#querying
measure_time(q1, spark, "Q1")
measure_time(q3, spark, "Q3")
measure_time(q5, spark, "Q5")
measure_time(q7, spark, "Q7")
measure_time(q16, spark, "Q16")
measure_time(q18, spark, "Q18")
measure_time(q20, spark, "Q20")
measure_time(q22, spark, "Q22")
