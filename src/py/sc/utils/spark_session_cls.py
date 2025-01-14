import os
from pyspark.sql import SparkSession

class SparkConnectSession():
    def __init__(self, remote:str, app_name:str, mode:str="connect", config: dict={}) -> None:
            if mode == "connect":
                self.ss = (SparkSession
                    .builder
                    .remote(remote)
                    .appName(app_name) 
                    .getOrCreate())
            elif mode == "classic":
                  self.ss = (SparkSession
                    .builder
                    .appName(app_name) 
                    .getOrCreate())
            elif mode == "dbconnect":
                  from databricks.connect.session import DatabricksSession as SparkSession
                  from databricks.sdk.core import Config
                  cluster_id = os.environ.get("clusterID")
                  config = Config(profile="default", cluster_id=cluster_id)
                  self.ss = SparkSession.builder.sdkConfig(config=config).getOrCreate()
            else:
                  raise Exception(f"Illegal mode: {mode} not supported")
    def get(self) ->SparkSession:
          return self.ss



