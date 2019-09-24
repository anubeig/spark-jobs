from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Find null")\
        .getOrCreate();

schema = StructType([
StructField('id', LongType(), False),
StructField('name', StringType(), True),
StructField('dept', StringType(), True),
StructField('deptid', LongType(), True)
])

def empRun():
    df = spark.createDataFrame([(1,'anu','csi',10), (2,'Bob','it',12),(3,'mogal','csi',10),(4,'baig','csi',10),(5,'ameer','ece',11),(6,'asma','ece',11)], schema)
    print("Table")
    print(df.show())
    df01 = df.groupBy(df.dept).agg(count(df.id).alias("count"))
    print("Finding no of employees in each dept")
    print(df01.show())
    df02 = df.groupBy(df.dept).agg(count(df.id).alias("count")).filter("count > 1")
    print("Finding dept which has more than one employee")
    print(df02.show())

