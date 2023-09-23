from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi
from pyspark.sql import SparkSession
import os

class Load:
    def __init__(self, DATA_PATH):
        self.path = DATA_PATH

    def load_procesing(self):
        ss = SparkSession.builder.appName('load').master('spark://spark-master:7077').getOrCreate()
        
        # Load data to parquet
        for filename in os.listdir(self.path+'/2. transform'):
            if filename.endswith('.json'):
                json_file_path = os.path.join(self.path+'/2. transform', filename)
                df = ss.read.json(json_file_path)
                df.toPandas().to_parquet(path=self.path+'/3. load/'+filename[:-5]+'.parquet', use_dictionary=False)
                print(f"Successfully load data to csv saved in {self.path+'/3. load/'+filename}")

        # Stop session spark
        ss.stop()
    