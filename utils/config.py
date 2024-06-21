from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
class Config:
    '''
    Configuration class for the project
    '''
    TBL_MEDIA = "tdm_dev.v_th_central_transaction_item_media"
    TBL_ITEM = "tdm.v_transaction_item"
    TBL_BASK = "tdm.v_transaction_head"
    TBL_PROD = "tdm.v_prod_dim"
    TBL_STORE = "tdm.v_store_dim"
    TBL_DATE = "tdm.v_th_date_dim"

    TBL_STORE_GR = "tdm.srai_std_rms_store_group_feed"
    TBL_SUB_CHANNEL = "tdm_seg.online_txn_subch_15aug"
    TBL_DHB_CUST_SEG = "tdm.dh_customer_segmentation"
    TBL_G_ID = "tdm.cde_golden_record"
    TBL_CUST = "tdm.v_customer_dim"
    TBL_CUST_PROFILE = "tdm.edm_customer_profile"
    TBL_MANUF = "tdm.v_mfr_dim"
    SRAI_PREF_STORE = "tdm.srai_prefstore_full_history"

    SPARK_PREFIX = "dbfs:/Volumes/prod/tdm_dev/edminput/filestore"
    FILE_PREFIX = "/Volumes/prod/tdm_dev/edminput/filestore"

    KEY_DIVISION = [1, 2, 3, 4, 9, 10, 13]
    KEY_STORE = [1, 2, 3, 4, 5]
    HDE_STORE = [1, 2, 3]
    TALAD_STORE = [4]
    GF_STORE = [5]        