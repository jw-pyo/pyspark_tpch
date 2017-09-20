##-*-encoding:utf-8-*-
q1 = """
SELECT  L_RETURNFLAG,
        L_LINESTATUS,
        sum(L_QUANTITY) as SUM_QTY,
        sum(L_EXTENDEDPRICE) as SUM_BASE_PRICE,
        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT)) as SUM_DISC_PRICE,
        sum(L_EXTENDEDPRICE * (1 - L_DISCOUNT) * (1 + L_TAX)) as SUM_CHARGE,
        avg(L_QUANTITY) as AVG_QTY,
        avg(L_EXTENDEDPRICE) as AVG_PRICE,
        avg(L_DISCOUNT) as AVG_DISC,
        count(*) as COUNT_ORDER
FROM    lineitem
WHERE   L_SHIPDATE <= '1998-09-16'
GROUP BY L_RETURNFLAG, L_LINESTATUS
ORDER BY L_RETURNFLAG, L_LINESTATUS
"""

q3 = """
SELECT  L_ORDERKEY,
        sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as REVENUE,
        O_ORDERDATE,
        O_SHIPPRIORITY
FROM    customer,
        orders,
        lineitem
WHERE   C_MKTSEGMENT = 'AUTOMOBILE'
AND     C_CUSTKEY = O_CUSTKEY
AND     L_ORDERKEY = O_ORDERKEY
AND     O_ORDERDATE < '1998-03-02'
AND     L_SHIPDATE > '1998-03-15'
GROUP BY L_ORDERKEY,
         O_ORDERDATE,
         O_SHIPPRIORITY
ORDER BY REVENUE desc,
         O_ORDERDATE
"""
q5 = """
SELECT  N_NAME,
        sum(L_EXTENDEDPRICE*(1-L_DISCOUNT)) as REVENUE
FROM    customer,
        orders,
        lineitem,
        supplier,
        nation,
        region
WHERE   C_CUSTKEY=O_CUSTKEY
AND     L_ORDERKEY=O_ORDERKEY
AND     L_SUPPKEY=S_SUPPKEY
AND     C_NATIONKEY=S_NATIONKEY
AND     S_NATIONKEY=N_NATIONKEY
AND     N_REGIONKEY=R_REGIONKEY
AND     R_NAME='ASIA'
AND     O_ORDERDATE >= '1998-03-02'
AND     O_ORDERDATE < '1999-03-02'
GROUP BY N_NAME
ORDER BY REVENUE desc
"""

q7 = "select SUPP_NATION, CUST_NATION, L_YEAR, SUM(VOLUME) AS REVENUE \
from ( SELECT \
N1.N_NAME AS SUPP_NATION, \
N2.N_NAME AS CUST_NATION, \
YEAR(L_SHIPDATE) AS L_YEAR, \
L_EXTENDEDPRICE * (1 - L_DISCOUNT) AS VOLUME \
FROM SUPPLIER, LINEITEM, ORDERS, CUSTOMER, NATION N1, NATION N2 \
WHERE S_SUPPKEY = L_SUPPKEY \
AND O_ORDERKEY = L_ORDERKEY \
AND C_CUSTKEY = O_CUSTKEY \
AND S_NATIONKEY = N1.N_NATIONKEY \
AND C_NATIONKEY = N2.N_NATIONKEY \
AND ((N1.N_NAME = 'KENYA' AND N2.N_NAME = 'PERU') \
OR (N1.N_NAME = 'PERU' AND N2.N_NAME = 'KENYA')) \
AND L_SHIPDATE BETWEEN '1995-01-01' AND '1996-12-31' \
) AS SHIPPING \
group by SUPP_NATION, CUST_NATION, L_YEAR \
order by SUPP_NATION, CUST_NATION, L_YEAR"

q_7 = """
SELECT SUPP_NATION, CUST_NATION, L_YEAR, sum(VOLUME) as REVENUE
FROM (SELECT n1.N_NAME as SUPP_NATION, n2.N_NAME as CUST_NATION, extract(YEAR(L_SHIPDATE)) as L_YEAR, L_EXTENDEDPRICE*(1-L_DISCOUNT) as VOLUME
FROM supplier, lineitem, orders, customer, nation n1, nation n2
WHERE S_SUPPKEY = L_SUPPKEY
AND O_ORDERKEY = L_ORDERKEY
AND C_CUSTKEY = O_CUSTKEY
AND S_NATIONKEY = n1.N_NATIONKEY
AND C_NATIONKEY = n2.N_NATIONKEY
AND ( (n1.N_NAME='FRANCE' AND n2.N_NAME='GERMANY') OR (n1.N_NAME='GERMANY' AND n2.N_NAME='FRANCE') )
AND L_SHIPDATE BETWEEN '1995-05-01' AND '1996-12-31') AS SHIPPING
GROUP BY SUPP_NATION, CUST_NATION, L_YEAR
ORDER BY SUPP_NATION, CUST_NATION, L_YEAR;
"""

q16 = """
SELECT  P_BRAND,
        P_TYPE,
        P_SIZE,
        count(distinct PS_SUPPKEY) as SUPPLIER_CNT
FROM    partsupp,
        part
WHERE   P_PARTKEY = PS_PARTKEY
AND     P_BRAND <> 'Brand#45'
AND     P_TYPE not like 'MEDIUM POLISHED%'
AND     P_SIZE in (49, 14, 23, 45, 19, 3, 36, 9)
AND     PS_SUPPKEY not in ( SELECT S_SUPPKEY FROM supplier WHERE S_COMMENT like '%Customer%Complaints%' )
GROUP BY    P_BRAND,
            P_TYPE,
            P_SIZE
ORDER BY    SUPPLIER_CNT desc,
            P_BRAND,
            P_TYPE,
            P_SIZE
"""
q18 = """
SELECT  C_NAME,
        C_CUSTKEY,
        O_ORDERKEY,
        O_ORDERDATE,
        O_TOTALPRICE,
        sum(L_QUANTITY)
FROM    customer,
        orders,
        lineitem
WHERE   O_ORDERKEY in ( SELECT L_ORDERKEY FROM lineitem GROUP BY L_ORDERKEY HAVING sum(L_QUANTITY) > 300 )
AND     C_CUSTKEY = O_CUSTKEY
AND     O_ORDERKEY = L_ORDERKEY
GROUP BY    C_NAME,
            C_CUSTKEY,
            O_ORDERKEY,
            O_ORDERDATE,
            O_TOTALPRICE
ORDER BY    O_TOTALPRICE desc,
            O_ORDERDATE
"""
q20 = """
SELECT S_NAME, S_ADDRESS FROM supplier, nation WHERE S_SUPPKEY in ( SELECT PS_SUPPKEY FROM partsupp WHERE PS_PARTKEY in ( SELECT P_PARTKEY FROM part WHERE P_NAME like 'forest%' ) and PS_AVAILQTY > ( SELECT 0.5 * sum(L_QUANTITY) FROM lineitem WHERE L_PARTKEY = PS_PARTKEY AND L_SUPPKEY = PS_SUPPKEY AND L_SHIPDATE >= date('1994-01-01') and L_SHIPDATE < date('1995-01-01') )) AND S_NATIONKEY = N_NATIONKEY AND N_NAME = 'CANADA' ORDER BY S_NAME
"""

q_22 = """
SELECT CNTRYCODE, count(*) as NUMCUST, sum(C_ACCTBAL) as TOTACCTBAL from ( select substr(C_PHONE 1, 2) as CNTRYCODE, C_ACCTBAL from customer where substr(C_PHONE 1 , 2) in ('13','31’,'23','29','30','18','17') and C_ACCTBAL > ( select avg(C_ACCTBAL) from customer where C_ACCTBAL > 0.00 and substr (C_PHONE 1,  2) in ('13','31','23','29','30','18','17') ) and not exists ( select * from orders where O_CUSTKEY = C_CUSTKEY ) ) as CUSTSALE group by CNTRYCODE order by CNTRYCODE
"""



q22 = """
select cntrycode, count(*) as numcust, sum(c_acctbal) as totacctbal
from ( select substr(c_phone, 1, 2) as cntrycode, c_acctbal
from customer
where substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
and c_acctbal > ( select avg(c_acctbal)
from customer
where c_acctbal > 0.00 and substr(c_phone, 1, 2) in ('13','31','23','29','30','18','17')
) and not exists ( select * from orders
where o_custkey = c_custkey
)
) as custsale
group by cntrycode
order by cntrycode
"""




