#Pyspark - Built-In functions : Window Functions:rank,col,min,max,row_number,max,asc,desc,when,count,sum

#trim() --> remove leading and trilling spaces

##Find Maximum and Minimum Salary in each department by using PySpark
data= [("James", "Sales", 2000),
("sofy", "Sales", 3000),
("Laren", "Sales", 4000),
("Kiku", "Sales", 5000),
("Sam", "Finance", 6000),
("Samuel", "Finance", 7000),
("Yash", "Finance", 8000),
("Rabin", "Finance", 9000),
("Lukasz", "Marketing", 10000),
("Jolly", "Marketing", 11000),
("Mausam", "Marketing", 12000),
("Lamba", "Marketing", 13000),
("Jogesh", "HR", 14000),
("Mannu", "HR", 15000),
("Sylvia", "HR", 16000),
("Sama", "HR", 17000),
]

#Schema

emp_schema = StructType([StructField("name",StringType()), StructField("dept_name",StringType()),StructField("Salary", StringType())])

employees_Salary_df = spark.createDataFrame(data, emp_schema)

###Ans
max_min_salary_dpt = Window.partitionBy('dept_name')
row_part = Window.partitionBy("dept_name").orderBy(col("Salary").desc())
employees_Salary_df.withColumn('max_salary', max(col('salary')).over(max_min_salary_dpt))\
    .withColumn('min_salary', min(col('salary')).over(max_min_salary_dpt))\
        .withColumn('rn',row_number().over(row_part))\
        .filter(col('rn')==1)\
            .select('dept_name','max_salary','min_salary').display()

################################################
##ROW_NUMBER(), RANK(), DENSE_RANK()
row_partition = Window.partitionBy('dept_Name').orderBy(col('Salary').asc())
employees_Salary_df.withColumn('Row_number' , row_number().over(row_partition))\
    .withColumn('Rank' , rank().over(row_partition))\
        .withColumn('DENS_RANK' , dense_rank().over(row_partition))\
            .withColumn('PERCENT_RANK' , percent_rank().over(row_partition)).display()

################################################
##Lag(), LEAD()
partition_by = Window.partitionBy('dept_Name').orderBy('salary','name')

employees_Salary_df.withColumn('privios_salary', lag(col('salary'),1,0).over(partition_by))\
    .withColumn('privios_salary_2', lag(col('salary'),2,0).over(partition_by))\
        .withColumn('tril_salary', lead(col('salary'),1,0).over(partition_by))\
            .withColumn('tril_salary_2', lead(col('salary'),2,0).over(partition_by)).display()
            
#################################################
# Aggregate functions examples
windowSpecAgg  = Window.partitionBy("dept_name")
from pyspark.sql.functions import col,avg,sum,min,max,row_number

employees_Salary_df.withColumn('avg_salary', avg('salary').over(windowSpecAgg))\
    .withColumn('total_salary', sum('salary').over(windowSpecAgg))\
        .withColumn('max_salary', max('salary').over(windowSpecAgg))\
            .withColumn('min_salary', min('salary').over(windowSpecAgg))\
                .withColumn('row_number', row_number().over(Window.partitionBy('dept_Name').orderBy('max_salary','min_salary'))).display()