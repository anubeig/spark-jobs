#Find the highest employee salary of the employee in each department in each loc

"""
+---+---+----+-----+
|eid|loc|dept|  sal|
+---+---+----+-----+
|  1|hyd| Adm|10000|
|  2|hyd|  IT|12000|
|  3|hyd| Adm|20000|
|  4|hyd|  IT|10000|
|  5|ban| Adm|30000|
|  6|ban|  IT|12000|
|  7|ban| Adm|20000|
|  8|ban|  IT|10000|
+---+---+----+-----+

Find the highest agg salary of dept of each location.
"""


from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import Window as window

schema = StructType([
StructField('eid', LongType(), False),
StructField('loc',StringType(),False),
StructField('dept', StringType(), True),
StructField('sal',LongType(), True),

])

def deptRun(spark):
    df = spark.createDataFrame([(1,'hyd','Adm',10000),
                            (2,'hyd','IT', 12000),
                            (3,'hyd','Adm',20000),
                            (4,'hyd','IT', 10000),
                            (5,'ban', 'Adm', 30000),
                            (6,'ban', 'IT', 12000),
                            (7,'ban', 'Adm', 20000),
                            (8,'ban', 'IT', 10000)], schema)
    print("Table")
    print(df.show())

    print(df.select(df.loc,df.dept,df.sal, rank().over(window.partitionBy(df.loc,df.dept).orderBy(df.sal.desc())).alias('rank')).filter('rank=1').drop('rank').show())

    """
    +---+----+-----+
    |loc|dept|  sal|
    +---+----+-----+
    |hyd|  IT|12000|
    |ban|  IT|12000|
    |hyd| Adm|20000|
    |ban| Adm|30000|
    +---+----+-----+
    """

    #Without Windows functions

    df1 = df.groupby(df.loc,df.dept).agg(max(df.sal).alias("sal"))
    df1 = df.join(df1,((df1.loc == df.loc) & (df1.dept == df.dept) & (df1.sal == df.sal))).select(df.eid,df.loc,df.dept,df.sal)
    print(df1.show())

    """
    +---+---+----+-----+
    |eid|loc|dept|  sal|
    +---+---+----+-----+
    |  5|ban| Adm|30000|
    |  3|hyd| Adm|20000|
    |  2|hyd|  IT|12000|
    |  6|ban|  IT|12000|
    +---+---+----+-----+
    """