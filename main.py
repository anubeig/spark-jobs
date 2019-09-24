import sys
import jobs.EmpJob as e
import jobs.DeptJob as d
from pyspark.sql import SparkSession

spark = SparkSession\
        .builder\
        .appName("Jobs to Test")\
        .getOrCreate();

if __name__ == "__main__":
    jobName =  sys.argv[1]
    print("==========================" + jobName)
    if jobName == 'empJob':
        e.empRun(spark)
    elif jobName == 'deptJob':
        d.deptRun(spark)

