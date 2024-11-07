# Databricks notebook source
import json
import os
import time
from datetime import datetime


# COMMAND ----------

class DatabricksWriteStats: 
    notebook_full_path = dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()
    # notebook_name = os.path.basename(notebook_full_path)

    def _write_to_log(self, record:dict): 
        stats_table = 'edw_audit.house_keeping.etl_write_statistics'
        row_schema = [
            "target_system:string", "target_full_object:string", "save_mode:string", 
            "source_count:int", "target_after_count:int", 
            "time_elapsed_in_sec:float", 
            "notebook_path:string", "load_status:string", 
            "EDW_INSERT_DATETIME:timestamp"]
        spark.createDataFrame([record], schema=", ".join(row_schema)).write \
                                       .format("delta") \
                                       .mode("append") \
                                       .option("overwriteSchema", "True") \
                                       .saveAsTable(stats_table)

    def __init__(self, target_full_object, target_system:str = 'Lakehouse'):
        self.target_system = target_system        
        self.target_full_object = target_full_object
        self.start_time = 0
        self.end_time = 0
        self.total_time = 0

    def start_timer(self):
        self.start_time = time.time()

    def end_timer(self): 
        self.end_time = time.time()
        self.total_time = self.end_time - self.start_time
    
    def insert_stats_log(self, source_count:int, target_after_count:int, save_mode:str='overwrite', load_status:str="succeeded"):
        body_dict = {
            "target_system": self.target_system,
            "target_full_object": self.target_full_object,
            "save_mode": save_mode,
            "source_count": source_count,
            "target_after_count": target_after_count,
            "time_elapsed_in_sec": self.total_time,
            "notebook_path": self.notebook_full_path,
            "load_status": load_status,
            "EDW_INSERT_DATETIME": datetime.now()
        }
        self._write_to_log(body_dict)



# COMMAND ----------


