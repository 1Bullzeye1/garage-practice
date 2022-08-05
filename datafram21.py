from pyspark.sql import *
from itertools import *
from pyspark.sql.types import *
spark = SparkSession.builder.master("local[*]").appName("hive").getOrCreate()

list1 = [1,2,3,4,5]
rdd1 = spark.sparkContext.parallelize(list1)
print(rdd1.collect())



data_400x = [(1,'k#nal','kunal@gmail.com' ,'100#0'),
          (2,'kun#al','kunal@gmail.com' ,'2000'),
          (3,'k@nal','k@@nalgm#ail.com' ,'2000'),
          (4,'k@nal','k@@nal@gma##il.com@@' ,'2000'),
         ]
l= spark.createDataFrame( data_400x , StringType() ).rdd.map( lambda x : x ).collect()[0]['value']
[ [z for z in y] for y in chain( *[combinations( l , n) for n in range(1 , len(l) + 1 )])]