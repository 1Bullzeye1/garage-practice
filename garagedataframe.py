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
customer = cust_.withColumn("CJ_DATE",to_date(cust_["CJ_DATE"],'yyyy/MM/dd'))
# customer.show()

emp_ = spark.read.csv('/home/mike/garage/employee.csv',header=True,inferSchema=True)
employee = emp_.withColumn("EDOJ",to_date(emp_["EDOJ"],'d-M-yyyy')).withColumn("EDOL",to_date(emp_["EDOL"],'d-M-yyyy'))
employee.show()

pur_ = spark.read.csv('/home/mike/garage/purchase.csv',header=True,inferSchema=True)
purchase = pur_.withColumn("PDATE",to_date(pur_["PDATE"],'d-M-yyyy'))
purchase.show()
# purchase.printSchema()

ser_ = spark.read.csv('/home/mike/garage/ser_det.csv',header=True,inferSchema= True)
service = ser_.withColumn("SER_DATE",to_date(ser_["SER_DATE"],'d-M-yyyy'))
service.show()
# service.printSchema()

spare = spark.read.csv('/home/mike/garage/sparepart.csv',header=True,inferSchema=True)
spare.show()
# spa_.printSchema()

ven_ = spark.read.csv('/home/mike/garage/vendors.csv',header=True,inferSchema=True)
vendors = ven_.withColumn("VJ_DATE",to_date(ven_["VJ_DATE"],'dd-MM-yyyy'))
vendors.show()
# vendors.printSchema()

customer.createOrReplaceTempView("customer")
employee.createOrReplaceTempView("employee")
purchase.createOrReplaceTempView("purchase")
service.createOrReplaceTempView("service")
spare.createOrReplaceTempView("sparepart")
vendors.createOrReplaceTempView("vendors")


 #qwe1)list all the customer service

#--------sql-----------
spark.sql("select c.CID,c.CNAME,s.TYP_SER from customer c join service s on (c.CID = s.CID)").show()

df1 =customer.join(service, customer.CID == service.CID )\
    .select(customer.CID,customer.CNAME,service.TYP_SER)
df1.show()

# qwe2)customer who are not serviced

df2=customer.join(service,customer.CID ==service.CID,"leftanti")
df2.show(truncate=False)

#----sql---------------

spark.sql('''select c.CID,c.CNAME,s.TYP_SER from customer c left join service s on c.CID =s.CID 
     where s.CID is null''').show()

#QWE3)EMPLOYEE WHO HAVE NOT RECEIVED THE COMMISSION:

df3=service.join(employee,service.EID == employee.EID,"right").filter(service.COMM==0)
df3.show()

    #qwe4)name the employee who have max commission
    # ese2 = df7.join(df3,df7.EID==df3.EID,'inner').select(max('comm1').alias("max")).show()
    # ese2.join(df7, df7.comm1 == ese2.max, 'inner').select('*').show()

    # tp1 = ese2.join(df7, df7.comm1 == ese2.max, 'inner').select('*')
    # tp1.join(df3, df3.EID == tp1.EID).select([df3.ENAME, tp1.comm1]).distinct().show()
    # ese2=df5.join(df3,df3.EID == df5.EID,"right").groupby("EID").max(df5.COMM).show()
    #df5.groupBy("EID").max("COMM").show()
    #ese1=spark.sql("select EID,max("COMM") from ser_detpy group by EID").shows

    #ese2= df3.filter(df3.EID.isin(df7.groupBy(col("EID")).max("COMM").collect()[0][0])).show()

    #que5)Show employee name and minimum commission amount received by an employee.
    # df6.show()
    # ese2=df7.join(df3,df3.EID == df7.EID,'inner').select(min('comm1').alias("abc")).show()
    # #ese2.join(df7,df7.comm1==ese2.abc,'inner').select('*')
    # tp1= ese2.join(df7,df7.comm1==ese2.abc,'inner').select('*')
    # tp1.join(df3,df3.EID==tp1.EID).select([df3.ENAME,tp1.comm1]).distinct().show()

    #ese1=spark.sql("select distinct employee11py.ENAME,ser.comm1 from employee11py join ser using (EID) where ser.comm1=(select max(comm1) from ser)").show()



    #que6)Display the Middle record from any table.
    # ese1=spark.sql("select * from (select C.* ,ROWNUM RNM from customerpy C) where RNM=(select round (count(*)/2)from customerpy").show()

    #ese1=spark.sql("select * from employee11py where ROWNUM <= (select (count(*)/2) from employee11py").show()


    #quw7)Display last 4 records of any table.
    #spark.sql("select * from (rank() over (order by CID desc) as rank from customerpy c)x where x.rank < 4  ").show()
    # df3.orderBy(col('EID').desc()).show(4)
    # df3.tail(4)
    # df3.take(4)
    # df3.limit(4)
    # ese1=spark.sql(("select * from customerpy order by CID desc")).show(4)


    #que8)Count the number of records without count function from any table.
    # x=df3.select(count('EID')).show()
    # df3.select(length('EID')).show()

    # ese1=spark.sql("select count('EID') FROM employee11py").show()


    #que9)Delete duplicate records from "Ser_det" table on cid.(note Please rollback after execution)


    #10)Show the name of Customer who have paid maximum amount
    #ese2=df1.join(df5,df1.CID==df5.CID,'inner').select(max(col('SP_AMT'))).show()

    # ESE1=spark.sql("select customerpy.CNAME,ser_detpy.SP_AMT from customerpy join ser_detpy where ser_detpy.SP_AMT=(select max(SP_AMT) from ser_detpy)" ).show()

    #Q.11 Display Employees who are not currently working.
    #ese2=df3.select(['EDOL','ENAME']).filter("EDOL is null").show()
    #ese1=spark.sql("select ENAME,EDOL FROM employee11py where EDOL is null").show()

    # Q.12 How many customers serviced their two wheelers.
    # ese1=spark.sql("select count(*) from (select TYP_VEH,TYP_SER,count(CID) FROM ser_detpy  WHERE TYP_VEH = 'TWO WHEELER' group by TYP_VEH,TYP_SER )")
    # ese1.show()

    # ese1 = spark.sql("select count(*) from (select TYP_VEH,count(CID) FROM ser_detpy  group by TYP_VEH,CID having TYP_VEH = 'TWO WHEELER' )")
    # ese1.show()
# ese2=df5.select('CID','TYP_VEH').filter(col("TYP_VEH") == 'TWO WHEELER').groupby(col("CID"),col("TYP_VEH")).count().count()
# print(ese2)



# Q.13 List the Purchased Items which are used for Customer Service with Unit of that Item.

# Q.14 Customers who have Colored their vehicles.
# ese1=spark.sql("select TYP_SER,CID from ser_detpy where TYP_SER= 'COLOR'").show()
# ese2=df5.select('TYP_SER','CID').filter(col("TYP_SER") == 'COLOR').show()


# Q.15 Find the annual income of each employee inclusive of Commission
 #Q.16 Vendor Names who provides the engine oil.
# ese2=df6.join(df2,df6.VID==df2.VID,'INNER').select(df2.VNAME,df6.VID,df6.SPID)
# ese4=ese2.join(df4,ese2.SPID=df4.SPID,'inner').f

# ese1=spark.sql("select purchasepy.VID,vendorpy.VNAME,purchasepy.SPID from purchasepy join vendorpy")
# ese5=spark.sql("select sparepart.SPNAME,ese1.SPID from ese1 join sparepart")
# Q.17 Total Cost to purchase the Color and name the color purchased.
# ese2=df4.select('SPNAME','SPRATE').where(col('SPNAME').like("%COLOUR")).show()
# ese1=spark.sql("select SPNAME,SPRATE from sparepartpy where SPNAME like '%COLOUR'").show()

# Q.18 Purchased Items which are not used in "Ser_det".

#ese2=df5.join(df4,df5.SPID==df4.SPID,'leftanti').show() #.filter(df4.SPID == 'null')

# Q.19 Spare Parts Not Purchased but existing in Sparepart
##same logic
# Q.20 Calculate the Profit/Loss of the Firm. Consider one month salary of each employee for Calculation.

#Q.21 Specify the names of customers who have serviced their vehicles more than one time.
# ese2=df5.select('CID','TYP_SER').groupby(col("CID"),col("TYP_SER")).count()
# df8=ese2.filter("count('CID') > 1").show()
# ese1=spark.sql("select TYP_SER,CID,count(CID) from ser_detpy group by TYP_SER,CID having count(CID) > 1").show()

# Q.22 List the Items purchased from vendors locationwise.
# ese1=spark.sql('''select VADD ,count(*) from (select purchasepy.PID,vendorpy.VADD,count(vendorpy.VADD)
# from purchasepy join vendorpy using(VID) group by vendorpy.VADD,purchasepy.PID) group by (VADD)''')
# ese1.show()

# ese2=df2.join(df6,df2.VID == df6.VID,'inner').groupby(df2.VADD).count()
# ese2.show()



# Q.23 Display count of two wheeler and four wheeler from ser_details

# ese1=spark.sql("select TYP_VEH,count(*) from ser_detpy group by TYP_VEH").show()

# ese2=df5.groupby('TYP_VEH').count()
# ese2.show()

# Q24 Display name of customers who paid highest SPGST and for which item

# ese2=df1.join(df5,df5.CID==df1.CID,'full')
# ese3=ese2.join(df6,ese2.SPID==df6.SPID,'full').select(df6.SPGST,ese2.CNAME).orderBy(asc_nulls_last(df6.SPGST)).show(1)

#ese1=spark.sql('''select customerpy.CID,purchasepy.SPGST,customerpy.CNAME from ser_detpy full join customerpy on (ser_detpy.CID = customerpy.CID)
            #full join purchasepy on (purchasepy.SPID = ser_detpy.SPID ) where SPGST= (select max(SPGST) from purchasepy)''').show()

# Q25 Display vendors name who have charged highest SPGST rate  for which item


# Q26list name of item and employee name who have received item
# ese1=spark.sql('''select * from purchasepy full join sparepartpy using (SPID)
#             full join employee11py on purchasepy.RCV_EID == employee11py.EID''').show()

# ese2=df3.join(df6,df3.EID==df6.RCV_EID).select(df6.RCV_EID,df6.SPID,df3.EID,df3.ENAME)
# ese3=ese2.join(df4,ese2.SPID==df4.SPID).select(df4.SPNAME,ese2.ENAME).show()

#Q27 Display the Name and Vehicle Number of Customer who serviced his vehicle, And Name the Item used for Service, And specify the purchase ded

# date of that Item with his vendor and Item Unit and Location, And employee Name who serviced the vehicle. for Vehicle NUMBER "MH-14PA335".'
#Q28 who belong this vehicle  MH-14PA335" Display the customer name
#ese1=df5.join(df1,df5.CID==df1.CID).select(df1.CNAME,df5.VEH_NO,df5.TYP_VEH).where(col('VEH_NO')== 'MH-14PA335').show()
#ese2=spark.sql("select customerpy.CNAME,ser_detpy.TYP_VEH,ser_detpy.VEH_NO from ser_detpy join customerpy ").show()

# ese2=spark.sql("select VEH_NO, CID ,TYP_VEH from (select * FROM ser_detpy  )")
# ese3=spark.sql("select CNAME,CID from customerpy ")
# ese4=spark.sql("select VEH_NO,CID from ese3 join ese2 using (CID) where VEH_NO = 'MH-14PA335' ")

# spark.sql('''select c.CNAME,s.CID,s.VEH_NO,s.TYP_VEH from customerpy c inner join ser_detpy s
#                 on (s.CID = c.CID) where s.VEH_NO = "MH-14PA335"''').show()
#ese4=spark.sql("select ese2.VEH_NO,ese2.TYP_VEH,ese3.CNAME from ese2 join ese3 using(CID)").show()

# Q29 Display the name of customer who belongs to New York and when he /she service their  vehicle on which date
# spark.sql("select c.CID,c.CADD,s.CID,s.SER_DATE from customerpy c inner join ser_detpy s  on c.CID == s.CID where c.CADD = 'NEW YORK' ").show()
# ese2=df1.join(df5,df1.CID==df5.CID,'inner').select(df1.CID,df1.CADD,df5.CID,df5.TYP_SER,df5.SER_DATE).where(df1.CADD == 'NEW YORK').show()

# Q 30 from whom we have purchased items having maximum cost?

#ese1=spark.sql("select v.VNAME, v.VID,p.VID,p.TOTAL) from purchasepy p join vendorpy v on v.VID == p.VID  group by p.VID,p.TOTAL,v.VNAME,v.VID ").show()
#ese2=df2.join(df6, df2.VID == df6.VID).select(df2.VNAME,df6.TOTAL).select(max(df6.TOTAL)).show()
#ese3=spark.sql("select P.VID,P.TOTAL,v.VNAME,v.VID from purchasepy P join vendorpy v  on P.VID == v.VID where P.TOTAL =(select MAX(TOTAL) FROM purchasepy)").show()

# Q31 Display the names of employees who are not working as Mechanic and that employee done services
# ese2=spark.sql("select e.ENAME,e.EJOB from employee11py e join ser_detpy s on e.EID == s.EID where e.EJOB != 'MECHANIC' ").show()
# ese1=df3.join(df5,df3.EID==df5.EID).select(df3.ENAME,df3.EJOB).filter(df3.EJOB != 'MECHANIC').show()

# Q32 Display the various jobs along with total number of employees in each job. The output should
# contain only those jobs with more than two employees.
# ese2=spark.sql("select EJOB,count(*) from employee11py group by EJOB having count(1) > 2").show()
# ese1=df3.select('ENAME','EJOB').groupby('EJOB').count()
# ese3=ese1.filter(col('count') > 2).show()

# Q33 Display the details of employees who done service  and give them rank according to their no. of services .
#ese2=spark.sql("select e.EID,e.ENAME,e.EJOB,s.TYP_SER,count(s.TYP_SER) as row_num_desc from employee11py e,ser_detpy s  ").show()
#ese1=df5.withColumn("rank",rank().over(Window.orderBy('TYP_SER'))).show()
# ese1 = df3.join(df5,df3.EID == df5.EID).select(df3.EID,df3.ENAME,df3.EJOB,df5.TYP_SER)\
#     .withColumn("rank",dense_rank().over(Window.orderBy('TYP_SER')))
# ese1.show()

# Q 34 Display those employees who are working as Painter and fitter and who provide service and total count of service done by fitter and painter
#ese1=df3.join(df5,df3.EID == df5.EID).select(df3.ENAME,df3.EJOB,df5.TYP_SER).filter(df3.EJOB == 'FITTER' | df3.EJOB == 'PAINTER').show()
#ese2=spark.sql("select e.ENAME,e.EJOB,s.TYP_SER,count(*) from employee11py e join ser_detpy s on e.EID == s.EID group by e.ENAME,e.EJOB,s.TYP_SER having e.EJOB = 'FITTER' or e.EJOB ='PAINTER' ").show()

# Q35 Display employee salary and as per highest  salary provide Grade to employee
# ese1=df3.withColumn("grade", when(df3.ESAL <= 1200,"C")
#                                  .when(df3.ESAL <= 1800,"B")
#                                  .when(df3.ESAL <= 2500,"A")
#                                  .when(df3.ESAL.isNull() ,"")
#                                  .otherwise(df3.ESAL))
# ese1.show()

# ese2=spark.sql('''select ESAL, case
#                     when ESAL <= 1200 then "C"
#                     when ESAL <= 1800 then "B"
#                     WHEN ESAL <= 2300 THEN "A"
#                     ELSE " "
#                     END AS GRADE
#                     FROM EMPLOYEE11PY ''')
# ese2.show()

# Q36  display the 4th record of emp table without using group by and rowid
#ese1=df3.withColumn("4th",row_number().over(Window.orderBy('EID'))).where(col('4th') == 4).show()
#ese2=spark.sql('''select *,row_number() over(order by EID ) from employee11py ''').show()

# Q37 Provide a commission 100 to employees who are not earning any commission.
# ese1=df3.join(df5,df3.EID == df5.EID)
# ese2=ese1.select(df3.ENAME,df5.COMM).withColumn("COMM1",when(ese1.COMM == 0,"100")
#                  .otherwise(ese1.COMM))
# ese2.show()

# ese2=spark.sql('''select e.ESAL,e.ENAME,s.COMM, case
#     when s.COMM == 0 THEN 100
#     ELSE s.COMM
#     END AS COMM1
#     from employee11py e join ser_detpy s on e.EID== s.EID ''').show()

# ese2=spark.sql('''select e.ESAL,e.ENAME,s.COMM,decode
#                (s.COMM,0,100)COMM1 from employee11py e join ser_detpy s on e.EID== s.EID ''').show()

# select e.eid,e.ename,sd.comm,decode(sd.comm,0,100)new_comm from employee_tab e
# join ser_det sd on e.eid=sd.eid



# Q38 write a query that totals no. of services  for each day and place the results # in descending order

# ese1=df5.orderBy('SER_DATE').groupby("SER_DATE").count()
# ese1.show()
#
# ese2=spark.sql("select SER_DATE,TYP_SER ,count(*) from ser_detpy group by SER_DATE,TYP_SER").show()

#Q39 Display the service details of those customer who belong from same city
# ese2=df5.join(df1,df5.CID==df1.CID).select(df1.CADD,df5.TYP_SER).orderBy(df1.CADD).show()
# ese1=spark.sql("select c.CADD,s.TYP_SER from customerpy c join ser_detpy s on c.CID== s.CID order by s.TYP_SER").show()


# Q40 write a query join customers table to itself to find all pairs of
# customers service by a single employee
#q.41`List each service number follow by name of the customer who
#made  that service
#ese1=df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df5.SID).show()
# Q42 Write a query to get details of employee and provide rating on basis of  maximum services provide by employee  .Note (rating should be like A,B,C,D)
#ese1=df5.select('SID','EID').groupby('SID').max('SID').show()

# Q43 Write a query to get maximum service amount of each customer with their customer details ?
#df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df1.CADD,df5.SER_AMT).distinct().show()
# Q44 Get the details of customers with his total no of services ?
# ese1=df1.join(df5,df1.CID == df5.CID,'INNER').select(df1.CNAME,df1.CID,df5.SID).groupby('CID').count()
# ese1.show()

# Q45 From which location sparpart purchased  with highest cost ?
#ese2=df2.join(df6,df2.VID==df6.VID,'inner').select(df2.VADD,df6.SPRATE).groupby('VADD').max('SPRATE').show()

# Q46 Get the details of employee with their service details who has salary is null
#ese2=df3.join(df5,df3.EID==df5.EID,'full').select('ENAME','ESAL','TYP_VEH','TYP_SER').where(df3.ESAL == "null").show()

#ese1=spark.sql("select e.ESAL,e.ENAME,s.TYP_VEH,s.TYP_SER FROM employee11py e full join ser_detpy s on e.EID == s.EID where ESAL is null").show()

# Q47 find the sum of purchase location wise
# df2.join(df6,df2.VID == df6.VID,'inner').select(df2.VADD,df6.TOTAL).groupby('VADD').sum('TOTAL').show()
# spark.sql("select v.VADD,sum(p.TOTAL) from vendorpy v join purchasepy p on v.VID==p.VID group by VADD ").show()


# Q48 write a query sum of purchase amount in word location wise ?



# Q49 Has the customer who has spent the largest amount money has
# been give highest rating
# ese1=df5.withColumn("rating", when(df5.TOTAL <= 500,"C")
#                                  .when(df5.TOTAL <= 1000,"B")
#                                  .when(df5.TOTAL <= 1500,"A")
#                                  .otherwise(df5.TOTAL))
#
# ese1.groupby('CID','rating').max('TOTAL').show()


# Q50 select the total amount in service for each customer for which
# the total is greater than the amount of the largest service amount in the table


#ese3=df5.select(df5.TOTAL,df5.SER_AMT).where( df5.TOTAL > (df5.select('SER_AMT').max('SER_AMT'))).show()
#ese1=spark.sql("select TOTAL,SER_AMT from ser_detpy  where TOTAL > (SELECT MAX(SER_AMT) from ser_detpy)").show()

# Q51  List the customer name and sparepart name used for their vehicle and  vehicle type

# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese3=ese1.join(df4,ese1.SPID == df4.SPID).select(ese1.CNAME,df4.SPNAME,ese1.TYP_VEH).show()

# Q52 Write a query to get spname ,ename,cname quantity ,rate ,service amount for record exist in service table
# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese3=ese1.join(df3,df3.EID == ese1.EID).select('CNAME','ENAME','SP_RATE','SER_AMT').show()

# Q53 specify the vehicles owners who’s tube damaged.

#ese1=df1.join(df5,df1.CID == df5.CID,'inner').select('CNAME','VEH_NO','TYP_SER').where(col('TYP_SER') == 'TUBE DAMAGED').show()

# Q.54 Specify the details who have taken full service.
#ese1=df5.filter(col('TYP_SER') == 'FULL SERVICING').show()

# Q.55 Select the employees who have not worked yet and left the job.
# df3.join(df5,df3.EID == df5.EID,'full').select('ENAME','EDOL',df3.EID,df5.EID)\
#     .filter(col('EDOL') != 'null' and  df5.EID == 'null').show()

# Q.56  Select employee who have worked first ever.
#df3.select('ENAME','EDOJ').orderBy('EDOJ').show()

# Q.57 Display all records falling in odd date
#df3.select('ENAME','EDOJ').filter((col('EDOJ')/2)==0).show()
df57 = employee.select('*').filter((date_format(col("EDOJ"),'d'))%2==1)
# df57.show()

# Q.58 Display all records falling in even date

df58 = employee.select('*').filter((date_format(col("EDOJ"),'d'))%2==0)
# df58.show()

# spark.sql('''select * from employee where to_num((EDOJ,'dd'))%2 =0''').show()
# Q.59 Display the vendors whose material is not yet used.

df59 = purchase.join(service,purchase.SPID == service.SPID,'leftanti')\
        .select(purchase.VID,purchase.SPID).join(vendors,purchase.VID==vendors.VID,'inner')\
        .select(purchase.VID,vendors.VNAME,vendors.VADD)
# df59.show()
# Q.60 Difference between purchase date and used dsed date of spare part.

df60 = purchase.join(service,purchase.SPID == service.SPID).select(purchase.SPID,purchase.PDATE,service.SER_DATE).withColumn("difference",datediff(service.SER_DATE,purchase.PDATE))
df60.show()



#ASCH TP
# ese1=df1.join(df5,df1.CID == df5.CID,'full')
# ese2=df3.join(ese1,df3.EID == ese1.EID,'full').select('CNAME','TYP_VEH','TOTAL')\
#     .groupby('TYP_VEH').max('TOTAL').show()

#df1.filter((df1.CNAME).isin(['CYONA BLAKE','TOM HILL'])).show()


# ESE1=df3.withColumn('new_sal',df3.ESAL * 2).where(df3.ENAME == 'LUIS POPP').show()

# ese1=df3.select('ENAME','ESAL').withColumn("new_sal",\
#             when(df3.ENAME == 'LUIS POPP',df3.ESAL * 2 )\
#                 .otherwise(df3.ESAL)).show()
# data=[{ "column" :'col1',"value": 'val1'},
#       {"column" : 'col2', "value" : 'val2'},
#       {"column" :'col3', "value": 'val3'},
#       {"column" :"col4", "value":'val4'}]
# df=spark.createDataFrame(data=data)
# df.show()


# df.groupBy().pivot('column').agg(max('value')).show()

# df1=spark.read.csv(r"C:\Users\91952\Documents\samplee.csv",inferSchema=True,header=True)
# df1.show()
#
#
#
# df2=df1.groupBy().pivot('A').agg(max('B')).show()
# df3=df1.groupBy().pivot('C').agg(max('C')).show()
# df1.withColumn("combColumn", concat("df2","df3")).show()
# df1.withColumn("combColumn", concat("_2","_3")).show()




