#Pyspark - Built-In functions : lower(), trim(), udf(function, return type(default)

#trim() --> remove leading and trilling spaces

from pyspark.sql.functions import *
cinema_data = [[1,"war","great 3D",8.9],
               [2,"Science","fiction",8.5],
               [3,"irish","boring",6.2],
               [4,"Ice song","Fantacy",8.6],
               [5,"House Card","Interesting",9.1]]
cinema_header   = ["ID","Movie","description","rating"]

cinema_df = spark.createDataFrame(cinema_data,cinema_header)

cinema_df.filter((col('id')%2 !=0) & (lower(trim(col('description'))) != 'boring')).display()

#####################################################################
def mask_credit_card(num):
    return str(num)[0:4] + '********'+str(num)[-4:]

mask_udf = udf(mask_credit_card, StringType())

data = [(1,'Rahul',1234567891234567),(2,'Raj',1234567892345678),(3,'Priya',1234567893456789),(3,'Murti',1234567890123456)]
schema = "id int, name string, card_no long"
credit_df = spark.createDataFrame(data,schema)

credit_df.withColumn('mask_udf', mask_udf(col('card_no'))).display()
