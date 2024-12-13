from pyspark.sql import SparkSession

class SparkConnectSession():
    def __init__(self, remote:str, app_name:str) -> None:
            self.ss = (SparkSession
                .builder
                .remote(remote)
                .appName(app_name) 
                .getOrCreate())
            
    def get(self) ->SparkSession:
          return self.ss
            