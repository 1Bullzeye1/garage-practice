import findspark
findspark.init()

from pyspark.sql import *
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.master("local[*]").appName("hi").getOrCreate()

# print(spark)


emp = spark.read.csv(r"D:\empl.csv",header = True,inferSchema = True)
emp.printSchema()
emp.show()

df1 = emp.withColumn('hire_date',to_date(emp["HIRE_DATE"],"d-M-yyyy"))
df1.printSchema()
df1.show()

df2 = emp.select(emp.FIRST_NAME,emp.LAST_NAME,concat(emp.FIRST_NAME.substr(0,1),emp.LAST_NAME.substr(0,1)).alias("OUTPUT"))
df2.show(100)

emp.createOrReplaceTempView("emp")

spark.sql('''select first_name || ' ' || last_name as NAME,substr(first_name,1,1) 
            || '' || substr(last_name,1,1) as Initials from emp''').show(100)


df4 = emp.select(emp.FIRST_NAME,emp.LAST_NAME,emp.DEPARTMENT_ID,emp.SALARY).withColumn("denserank",dense_rank()\
                                 .over(Window.partitionBy(emp.DEPARTMENT_ID).orderBy(desc(emp.SALARY)))).filter(col("denserank") <= 3)
df4.show()