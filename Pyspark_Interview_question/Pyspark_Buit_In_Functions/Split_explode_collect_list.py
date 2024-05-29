#Pyspark - Built-In functions : split(), explode, collect_list

from pyspark.sql.functions import *

data = [[3,'Hero HF DLX Mudguard','Black|Red'],
        [4,'Hero HF DLX Wiser','Black'],
        [2,'Hero Super SPLD','Black|Red|Blue'],
        [1,'Bajaj CT100 Mudguard','Black|Red|Silver']]
clm_header =  ['ItemId', 'ItemName', 'VartName']

iteminfo_df = spark.createDataFrame(data, clm_header)

iteminfo_df.withColumn('new_cl', explode(split(iteminfo_df['VartName'], "\|"))).display()
o/p:
 
ItemId,ItemName,VartName
3,Hero HF DLX Mudguard,Black
3,Hero HF DLX Mudguard,Red
4,Hero HF DLX Wiser,Black

###For revert back to initial position:

iteminfo_df.groupBy('ItemName')\
    .agg(collect_list(col('VartName'))).display()