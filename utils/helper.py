from typing import List
from copy import deepcopy
from datetime import datetime, timedelta
import functools
import time

import os
import pandas as pd
from pandas import DataFrame as PandasDataFrame
from numpy import ndarray as numpyNDArray

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import DataFrame as SparkDataFrame

spark = SparkSession.builder.appName("helper").getOrCreate()

def timer(func):
    @functools.wraps(func)
    def wrapper_timer(*args, **kwargs):
        tic = time.perf_counter()
        value = func(*args, **kwargs)
        toc = time.perf_counter()
        elapsed_time = toc - tic
        print(f"{func.__name__} | Elapsed time: {elapsed_time:0.4f} seconds")
        return value
    return wrapper_timer

def sparkDataFrame_to_csv_filestore(sf: SparkDataFrame, csv_folder_name: str, prefix: str) -> str:
    """Save spark DataFrame in DBFS Filestores path, under specifided as single CSV file
    and return HTML for download file to local.
    
    Parameter
    ---------
    sf: pyspark.sql.DataFrame
        spark DataFrame to save
        
    csv_folder_name: str
        name of 'csv folder', result 1 single file will be under those filder
        
    prefix: str , 
        Spark API full path under FileStore, for deep hierarchy, use without '/' at back
        ex. 'dbfs:/FileStore/thanakrit/trader' , 'dbfs:/FileStore/thanakrit/trader/cutoff'
        
    Return
    ------
    :bool
        If success True, unless False
    """
    import os
    from pathlib import Path
    from pyspark.dbutils import DBUtils
    
    dbutils = DBUtils(spark)
    
    # auto convert all prefix to Spark API
    spark_prefix = os.path.join('dbfs:', prefix[6:])
    spark_file_path = os.path.join(spark_prefix, csv_folder_name)
    
    # save csv
    (sf
     .coalesce(1)
     .write
     .format("com.databricks.spark.csv")
     .option("header", "true")
     .save(spark_file_path)
    )
    
    # get first file name endswith .csv, under in result path
    files = dbutils.fs.ls(spark_file_path)
    csv_file_key = [file.path for file in files if file.path.endswith(".csv")][0]
    csv_file_name = csv_file_key.split('/')[-1]
    
    # rename spark auto created csv file to target csv filename
    dbutils.fs.mv(csv_file_key, os.path.join(spark_file_path, csv_folder_name))
    
    # HTML direct download link
    filestore_path_to_url = str(Path(*Path(spark_file_path).parts[2:]))
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url, csv_folder_name + '?o=2606104010931262')
    print(f'HTML download link : {html_url}\n')
    
    return html_url

def to_pandas(sf: SparkDataFrame) -> pd.DataFrame:
    """Solve DecimalType conversion to PandasDataFrame as string
    with pre-covert DecimalType to DoubleType then toPandas()
    automatically convert to float64
    
    Parameter
    ---------
    sf: pyspark.sql.DataFrame
        spark DataFrame to save
        
    Return
    ------
    :pandas.DataFrame
    """
    from pyspark.sql import types as T
    from pyspark.sql import functions as F
    
    col_name_type = sf.dtypes
    select_cast_exp = [F.col(c[0]).cast(T.DoubleType()) if c[1].startswith('decimal') else F.col(c[0]) for c in col_name_type]
    conv_sf = sf.select(*select_cast_exp)
    conv_df = conv_sf.toPandas()
    return conv_df  # type: ignore

def pandas_to_csv_filestore(df: PandasDataFrame, csv_file_name: str, prefix: str) -> str:
    """Save PandasDataFrame in DBFS Filestores path as csv, under specifided path
    and return HTML for download file to local.
    
    Parameter
    ---------
    df: pandas.DataFrame
         pandas DataFrame to save
        
    csv_folder_name: str
        name of 'csv folder', result 1 single file will be under those filder
        
    prefix: str ,
        Full file path FileStore, for deep hierarchy, use without '/' at back
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    Return
    ------
    : str
        If success return htlm download location, unless None
    """
    import os
    from pathlib import Path
    
    # auto convert all prefix to File API
    prefix = os.path.join('/dbfs', prefix[6:])
    
    # if not existed prefix, create prefix
    try:
        os.listdir(prefix)
    except:
        os.makedirs(prefix)
    
    # Write PandasDataFrame as CSV to folder
    filestore_path = os.path.join(prefix, csv_file_name)

    try:
        df.to_csv(filestore_path, index=False)
        
        #---- HTML direct download link
        filestore_path_to_url = str(Path(*Path(filestore_path).parts[3:]))
        html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url + '?o=2606104010931262')
        print(f'HTML download link : {html_url}\n')
        return html_url
    
    except:
        return ""
    
def get_vector_numpy(sf: SparkDataFrame, vec_col:str) -> numpyNDArray:
    """
    Collect & convert mllip.Vector to numpy NDArray in local driver
    
    Parameter
    ---------
    sf: SparkDataFrame
        source data
    vec_col: str
        name of column with data type as spark vector
    
    Return
    ------
    numpyNDArray
        retun to local driver
    
    """
    from pyspark.ml.functions import vector_to_array
    
    # Size of vector
    v_size = sf[[vec_col]].take(1)[0][0].toArray().shape[0]  # type: ignore
    
    # exported vector name
    vec_export_name = vec_col + '_'
    out_np = \
    (sf
     .withColumn(vec_export_name, vector_to_array(vec_col))  # type: ignore
     .select([F.col(vec_export_name)[i] for i in range(v_size)])
    ).toPandas().to_numpy()
    
    return out_np

def bucket_bin(sf: SparkDataFrame, splits: List, split_col: str) -> SparkDataFrame:
    """
    Bin data with definded splits, if data contain NULL will skip from bining
    Result bin description in column name 'bin'
    
    Parameter
    ---------
    sf: SparkDataFrame
        source data for binning
        
    splits: List
        list for use as splits point
        Always put -float("inf") at the beginning of list to capture all other negative value not in the defined range
        and put float("inf") and the ending of list to capture all other positive value not in the definded range
    
    split_col: str
        name of column to be used as binning
        
    Return
    ------
    SparkDataFrame
        Column `bin_id` for ordering from low (1) -> high (max) value
        Column `bin` detail description of each bin range [lower - upper) ; inclusive left , exclusive right 
    """
    from pyspark.ml.feature import Bucketizer
    
    import pandas as pd
    from copy import deepcopy
    
    #---- Create bin desc
    up_bound = deepcopy(splits)
    up_bound.pop(0)
    low_bound = deepcopy(splits)
    low_bound.pop(-1)
    
    #---- Creae SparkDataFram for mapping split_id -> bin
    bin_desc_list = [[i, '['+str(l)+' - '+str(w)+')'] for i,(l,w) in enumerate(zip(low_bound, up_bound))]
    
    bin_desc_df = pd.DataFrame(data=bin_desc_list, columns=['bin_id', 'bin'])
    bin_desc_sf = spark.createDataFrame(bin_desc_df)
    
    # here I created a dictionary with {index: name of split}
    # splits_dict = {i:splits[i] for i in range(len(splits))}

    #---- create bucketizer
    bucketizer = Bucketizer(splits=splits, inputCol=split_col, outputCol="bin_id")

    #---- bucketed dataframe
    bucketed = bucketizer.setHandleInvalid('skip').transform(sf)
    
    #---- map bin id to bin desc
    bucketed_desc = bucketed.join(F.broadcast(bin_desc_sf), 'bin_id') #.drop('bin_id')
            
    return bucketed_desc

def create_zip_from_dbsf_prefix(dbfs_prefix_to_zip: str, output_zipfile_name: str):
    """
    Create zip from all file in dbfs prefix and show download link
    The zip file will be written to driver then copy back to dbsf for download url generation
    Also the output zip file will be in the parent folder of source zip file

    Parameter
    ---------
    dbfs_prefix_to_zip: str
        path under DBFS FileStore, for deep hierarchy, use **without** '/' in front & back
        use full file API path style , always start with '/dbfs/FileStore'
        ex. '/dbfs/FileStore/thanakrit/trader' , '/dbfs/FileStore/thanakrit/trader/cutoff'
    
    output_zipfile_name: str
        name of zip file with suffix '.zip' ex. 'output.zip'
        
    """
    import os
    import zipfile
    import shutil
    from pathlib import Path

    #----- Mount a container of Azure Blob Storage to dbfs
    # storage_account_name='<your storage account name>'
    # storage_account_access_key='<your storage account key>'
    # container_name = '<your container name>'

    # dbutils.fs.mount(
    #   source = "wasbs://"+container_name+"@"+storage_account_name+".blob.core.windows.net",
    #   mount_point = "/mnt/<a mount directory name under /mnt, such as `test`>",
    #   extra_configs = {"fs.azure.account.key."+storage_account_name+".blob.core.windows.net":storage_account_access_key})

    #---- List all files which need to be compressed, from dbfs
    source_dbfs_prefix  = dbfs_prefix_to_zip
    source_filenames = [os.path.join(root, name) for root, dirs, files in os.walk(top=source_dbfs_prefix , topdown=False) for name in files]
    print(source_filenames)
    
    #---- Convert to Path object
#     source_dbfs_prefix = Path(os.path.join('/dbfs', 'FileStore', dbfs_prefix_to_zip))

#     with ZipFile(filename, "w", ZIP_DEFLATED) as zip_file:
#         for entry in source_dbfs_prefix.rglob("*"):
#             zip_file.write(entry, entry.relative_to(dir))
    
    #---- Directly zip files to driver path '/databricks/driver'
    #---- For Azure Blob Storage use blob path on the mount point, such as '/dbfs/mnt/test/demo.zip'
    # target_zipfile = 'demo.zip'
    
    with zipfile.ZipFile(output_zipfile_name, 'w', zipfile.ZIP_DEFLATED) as zip_f:
        for filename in source_filenames:
            zip_f.write(filename)
    
    #---- Copy zip file from driver path back to dbfs path
    
    source_driver_zipfile = os.path.join('/databricks', 'driver', output_zipfile_name)
    root_dbfs_prefix_to_zip = str(Path(dbfs_prefix_to_zip).parent)
    target_dbfs_zipfile = os.path.join(root_dbfs_prefix_to_zip, output_zipfile_name)
    shutil.copyfile(source_driver_zipfile, target_dbfs_zipfile)
    
    #---- HTML direct download link
    root_dbfs_prefix_to_zip_remove_first_2_parts = str(Path(*Path(root_dbfs_prefix_to_zip).parts[3:]))
    print(root_dbfs_prefix_to_zip)
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', root_dbfs_prefix_to_zip_remove_first_2_parts, output_zipfile_name + '?o=2606104010931262')
    print(f'HTML download link :\n{html_url}')
    
    return html_url

def adjust_gofresh_region(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        # print('Before main function')
        txn = func(*args, **kwargs)
        print('Mapping new region')
        adjusted_store_region =  \
        (spark.table('tdm.v_store_dim')
         .select('store_id', F.col('region').alias('store_region'))
         .withColumn('store_region', F.when(F.col('store_region').isin(['West','Central']), F.lit('West+Central')).otherwise(F.col('store_region')))
         .drop_duplicates()
        )
        txn = txn.drop('store_region').join(adjusted_store_region, 'store_id', 'left')
        return txn
    return wrapper

def get_lag_id(epoch_id: str, 
               lag_num: int, 
               full_year_count: int, 
               inclusive: bool = True):
    """Get lagging epoch_id
    """
    import math

    epoch_num = int(epoch_id)
    year_part = epoch_num//100
    remainder_part = epoch_num%100
    
    if inclusive:
        lag_num -= 1
    
    if remainder_part <= lag_num:
        new_year_part = year_part - math.ceil(lag_num/full_year_count)
        new_remainder_num = new_year_part*100 + remainder_part
        diff_remainder_num = math.ceil(lag_num/full_year_count)*full_year_count - lag_num
        new_remainder_num = new_remainder_num + diff_remainder_num
    else:
        new_remainder_num = epoch_num - lag_num
        
    return new_remainder_num

def get_lag_period_id(pr_id: str, 
                      lag_num: int, 
                      inclusive: bool = True):
    """Get lagging priod_id
    """
    lag_pr_id = get_lag_id(epoch_id=pr_id, lag_num=lag_num, full_year_count=12, inclusive=inclusive)        
    return lag_pr_id

def get_lag_wk_id(wk_id: str, 
                  lag_num: int,
                  inclusive: bool = True):
    """Get lagging week_id
    """
    lag_wk_id = get_lag_id(epoch_id=wk_id, lag_num=lag_num, full_year_count=52, inclusive=inclusive)        
    return lag_wk_id

# def get_lag_wk_id(wk_id: str, lag_num: int):
#     """Get lagging week_id, inclusive ending period
#     """
#     import math
    
#     wk_num = int(wk_id)
#     year_part = wk_num//100
#     wk_part = wk_num%100
#     lag_inc_num = lag_num - 1
    
#     if wk_part <= lag_inc_num:
#         new_year_part = year_part - math.ceil(lag_inc_num/52)
#         new_wk_num = new_year_part*100 + wk_part
#         diff_wk_num = math.ceil(lag_inc_num/52)*52 - lag_inc_num
#         new_wk_num = new_wk_num + diff_wk_num
#     else:
#         new_wk_num = wk_num - lag_inc_num
        
#     return new_wk_num

def get_leding_wk_id(wk_id: str, leding_num: int = 1):
    """Get leading week_id, inclusive start week_id
    """
    wk_num = int(wk_id)
    added_wk_num = wk_num + leding_num
    if added_wk_num % 100 > 52:
        leding_wk_num = wk_num + 100 - (52 - leding_num)
    else:
        leding_wk_num = added_wk_num
        
    return leding_wk_num