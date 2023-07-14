
from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder.appName("appName").enableHiveSupport().getOrCreate()

df1=spark.sql("SELECT * FROM aarya.one where empid IS NOT NULL")
df2=spark.sql("SELECT * FROM aarya.two where empid is not null")
df1.createOrReplaceTempView("emp")
df2.createOrReplaceTempView("emp_comp")
result=spark.sql("select emp.name,emp.empid from emp inner join emp_comp on emp.empid=emp_comp.empid")
result.collect()
