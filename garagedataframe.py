from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("N00bda").master("local[*]").getOrCreate()

schema = StructType([StructField("CID",IntegerType(),nullable=False),
                                             StructField("CNAME",StringType(),nullable=True),
                                             StructField("CADD",StringType(),nullable=True),
                                             StructField("C_CONTACT",IntegerType(),nullable=True),
                                             StructField("C_CREDITDAYS",IntegerType(),nullable=False),
                                             StructField("CJ_DATE",DateType(),nullable=True),
                                             StructField("SEX",StringType(),nullable=True)])
cust_ = spark.read.csv('/home/mike/garage/customer.csv',header=True,inferSchema=True)
cust_.show()
cust_.printSchema()

emp_ = spark.read.csv('/home/mike/garage/employee.csv',header=True,inferSchema=True)
emp_.show()
emp_.printSchema()

pur_ = spark.read.csv('/home/mike/garage/purchase.csv',header=True,inferSchema=True)
pur_.show()
pur_.printSchema()

ser_ = spark.read.csv('/home/mike/garage/ser_det.csv',header=True,inferSchema= True)
ser_.show()
ser_.printSchema()

spa_ = spark.read.csv('/home/mike/garage/sparepart.csv',header=True,inferSchema=True)
spa_.show()
spa_.printSchema()

ven_ = spark.read.csv('/home/mike/garage/vendors.csv',header=True,inferSchema=True)
ven_.show()
ven_.printSchema()

df = ven_.withColumn("VJ_DATE",ven_["VJ_DATE"].cast(DateType()))
df.show()