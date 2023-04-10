    
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import DataFrame as SparkDataFrame

def get_exposure(self, 
                    exposure_type: str, 
                    visit_multiplier: float):
    
    self.params["visit_multiplier"] = visit_multiplier
    
    if exposure_type == "store_lv":
        self.params["exposure_type"] = "store_lv"
        txn_x_store = self.txn.join(self.target_store, "store_id", "inner")
        visit = (txn_x_store
                    .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                    .agg(F.col("transaction_uid"))
                    ).collect()[0][0]
        exposure = visit*visit_multiplier
        return exposure
    
    elif exposure_type == "aisle_lv":
        self.params["exposure_type"] = "aisle_lv"
        txn_x_store_x_aisle = self.txn.join(self.target_store, "store_id", "inner").join(self.aisle_sku, "upc_id", "inner")
        visit = (txn_x_store_x_aisle
                    .where(F.col("date_id").between(F.col("c_start"), F.col("c_end")))
                    .agg(F.col("transaction_uid"))
                    ).collect()[0][0]
        exposure = visit*visit_multiplier
        return exposure
    
    else:
        self.params["exposure_type"] = "undefined"
        exposure = None
        return exposure