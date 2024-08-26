import os
os.system('cls')

from utils.Utility import get_conf

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType,IntegerType,DoubleType
from pyspark.sql.functions import current_timestamp, lit

cwd = os.path.dirname(os.path.realpath(__file__))

confPath = 'Config/Config.json'
Conf = get_conf(os.path.join(cwd,confPath))

''' Source Path '''
rawPath = os.path.join(Conf['PATH']['base_path'],Conf['Formula1']['SourcePath'])

''' Destination Path '''
destPath = os.path.join(Conf['PATH']['base_path'],Conf['Formula1']['TargetPath'])

def circuitDf(sourcePath, destPath):
    '''Schema'''
    circuit_Schema = StructType([
        StructField('circuitId',IntegerType(), False ),
        StructField('circuitRef',StringType(), True),
        StructField('name',StringType(),True),
        StructField('location',StringType(),True),
        StructField('country',StringType(), True),
        StructField('lat', DoubleType(),True),
        StructField('lng', DoubleType(),True),
        StructField('alt', DoubleType(),True),
        StructField('url', StringType(),True)
    ]
    )

    circuit_df = spark.read.format('csv').option('header','true').schema(circuit_Schema).load(sourcePath)


    ''' Rename Column'''
    new_column = ['circuit_ID', 'circuit_Ref', 'name', 'location', 'country', 'lat', 'lng', 'alt', 'url']


    for col, newCol in zip(circuit_df.columns, new_column):
        # print(f'Column:{col} --> New Column : {newCol}')
        circuit_df = circuit_df.withColumnRenamed(col, newCol)

    

    circuit_df = circuit_df.withColumn('ingest_time', current_timestamp())\
        .withColumn('env', lit('DEV'))

    number_of_partition = circuit_df.rdd.getNumPartitions()
    # print(number_of_partition)

    ''' Write the data'''
    # print(destPath)
    # circuit_df.write.csv()


def races(sourcePath, destPath):
    pass

if __name__ =='__main__':
    ''' Main '''
    spark = SparkSession.builder \
            .appName('formula1') \
            .master("local[2]") \
            .config("spark.executor.memory", '4g') \
            .getOrCreate()
    
    ''' Circuit File '''
    circuitDf(os.path.join(rawPath, 'circuits.csv'), os.path.join(destPath, 'Bronze\Circuit\\') )


