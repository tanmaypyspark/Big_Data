from pyspark.sql import SparkSession
import json
import os

class ReadCSV:
    
    def __init__(self,spark,path,header='false',inschema='false',schema=None):
        self.spark = spark
        self.path = path
        self.header = header
        self.inferSchema = inschema
        self.schema = schema

        if self.schema ==None:
            # print(self.create.read.format('csv').option('inferSchema',inschema).option('header',header).load(path))
            self.df =  self.spark.read.format('csv').option('inferSchema',self.inferSchema).option('header',self.header).load(self.path)
        else:
            self.df =  self.spark.read.format('csv').option('header',self.header).schema(self.schema).load(self.path)
    
    def display(self):
        return self.df.show()
    
    def limit(self,val):
        return self.df.limit(val)

class Spark:
    def __init__(self,appname, memory='2g'):
        self.appName = appname
        self.memory = memory
    @property
    def create(self):
        return SparkSession.builder \
        .appName(self.appName) \
        .master("local[2]") \
        .config("spark.executor.memory", self.memory) \
        .getOrCreate()
    
    def read_csv(self,path,header='false',inschema='false',schema=None):
        return ReadCSV(self.create,path,header,inschema,schema)
    
    @property
    def limit(self,val):
        return self.read_csv.limit(val)
    
    @property
    def fn(self):
        pass


def get_conf(path):
    if os.path.exists(path):
        with open(path,'r') as f:
            data = f.read()
        return json.loads(data)
    else:
        return {}


# from itertools import batched

# data = [1,2,3,4,5]

# print(list(batched(data,2)))
#output: [(1,2),(3,4),(5,)]
#data = "ABCDEF"
#print(list(batched(data,3)))
#op: [('A','B','C'),('D','E','F')]