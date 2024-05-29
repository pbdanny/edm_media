from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
class Config:
    '''
    Configuration class for the project
    '''
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.TBL_MEDIA = "tdm_dev.v_th_central_transaction_item_media"
        self.TBL_ITEM = "tdm.v_transaction_item"
        self.TBL_BASK = "tdm.v_transaction_head"
        self.TBL_PROD = "tdm.v_prod_dim"
        self.TBL_STORE = "tdm.v_store_dim"
        self.TBL_DATE = "tdm.v_th_date_dim"

        self.TBL_STORE_GR = "tdm.srai_std_rms_store_group_feed"
        self.TBL_SUB_CHANNEL = "tdm_seg.online_txn_subch_15aug"
        self.TBL_DHB_CUST_SEG = "tdm.dh_customer_segmentation"
        self.TBL_G_ID = "tdm.cde_golden_record"
        self.TBL_CUST = "tdm.v_customer_dim"
        self.TBL_CUST_PROFILE = "tdm.edm_customer_profile"
        self.TBL_MANUF = "tdm.v_mfr_dim"
        self.SRAI_PREF_STORE = "tdm.srai_prefstore_full_history"

        self.SPARK_PREFIX = "dbfs:/Volumes/prod/tdm_dev/edminput/filestore"
        self.FILE_PREFIX = "/Volumes/prod/tdm_dev/edminput/filestore"

        self.KEY_DIVISION = [1, 2, 3, 4, 9, 10, 13]
        self.KEY_STORE = [1, 2, 3, 4, 5]
        self.HDE_STORE = [1, 2, 3]
        self.TALAD_STORE = [4]
        self.GF_STORE = [5]