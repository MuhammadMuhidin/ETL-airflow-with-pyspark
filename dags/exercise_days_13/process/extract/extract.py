from pyspark.sql import SparkSession
import os

class Extract:
    def __init__(self, DATA_PATH):
        self.path = DATA_PATH

    def extract_processing(self):
        ss = SparkSession.builder.appName('extract').master('spark://spark-master:7077').getOrCreate()

        # Extract data to json
        for filename in os.listdir(self.path+'/0. raw'):
            if filename.endswith('.csv'):
                csv_file_path = os.path.join(self.path+'/0. raw', filename)
                df = ss.read.csv(csv_file_path, header='true', inferSchema='true')
                extract_to = os.path.join(self.path+'/1. extract', filename[:-4]+'-extracted.json')
                df.toPandas().to_json(extract_to, orient='records')
                print(f"Successfully extract data to json saved in {extract_to}")

        # Stop session spark
        ss.stop()