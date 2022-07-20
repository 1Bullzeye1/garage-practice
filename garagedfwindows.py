from pyspark.sql import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
spark = SparkSession.builder.appName("N00bda").master("local[*]").getOrCreate()

schema = StructType([StructField("CID",IntegerType(),nullable=False),
                                             StructField("CNAME",StringType(),nullable=True),
                                             StructField("CADD",StringType(),nullable=True),
                                             StructField("C_CONTACT",IntegerType(),nullable=True),
                                             StructField("C_CREDITDAYS",IntegerType(),nullable=False),
                                             StructField("CJ_DATE",StringType(),nullable=True),
                                             StructField("SEX",StringType(),nullable=True)])
# cust_ = spark.read.csv(r"D:\garage\customer.csv",header=True,schema = schema)
# cust_.show()
# cust_.printSchema()


# customer =  cust_.withColumn("CJ_DATE",to_date(cust_["CJ_DATE"],'yyyy/MM/dd'))

# emp_ = spark.read.csv(r"D:\garage\employee.csv",header=True,inferSchema=True)
# emp_.show()
# emp_.printSchema()
#
# employ = emp_.withColumn("EDOJ",to_date(emp_["EDOJ"],'d-M-yyyy'))
# employee = employ.withColumn("EDOL",to_date(emp_["EDOL"],'d-M-yyyy'))
# employee.show()
# employee.printSchema()

# pur_ = spark.read.csv(r"D:\garage\purchase.csv",header=True,inferSchema=True)
#
# purchase = pur_.withColumn("PDATE",to_date(pur_["PDATE"],'d-M-yyyy'))
# purchase.show()
# purchase.printSchema()

#
# ser_ = spark.read.csv(r"D:\garage\ser_det.csv",header=True,inferSchema= True)
#
# service = ser_.withColumn("SER_DATE",to_date(ser_["SER_DATE"],'d-M-yyyy'))
# service.printSchema()

# sparepart = spark.read.csv(r"D:\garage\sparepart.csv",header=True,inferSchema=True)
# sparepart.show()
# sparepart.printSchema()
#
ven_ = spark.read.csv(r"D:\garage\vendors.csv",header=True,inferSchema=True)

vendors = ven_.withColumn("VJ_DATE",to_date(ven_["VJ_DATE"],'d-M-yyyy'))
vendors.printSchema()