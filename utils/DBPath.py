from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os

from pandas import DataFrame as PandasDataFrame
from pyspark.sql import DataFrame as SparkDataFrame

class DBPath(_Path_):
    
    _flavour = _windows_flavour if os.name == 'nt' else _posix_flavour
    
    def __init__(self, input_file):
        if "dbfs:" in input_file:
            raise ValueError("DBPath accept only file API path style (path start with /dbfs/)")
        super().__init__()
        return
        
    def __repr__(self):
        return f"DBPath class : {self.as_posix()}"
    
    def file_api(self):
        if self.as_posix()[:8] != "/Volumes":
            rm_first_5_str = str(self.as_posix())[5:]
            return str("/dbfs"+rm_first_5_str)
        else:
            return str(self.as_posix())
    
    def spark_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("dbfs:"+rm_first_5_str)

def save_SparkDataFrame_to_csv_FileStore(sf: SparkDataFrame, 
                                         save_file_path: DBPath) -> str:
    """Save spark DataFrame in DBFS FileStore as single .csv file
    and return HTML for download file to local.
    
    Parameter
    ---------
    sf: pyspark.sql.DataFrame
        spark DataFrame to save
        
    save_file_path: DBPath
        target file to save with extension
        ex. DBPath'/dbfs/FileStore/media/campaign_eval/01_hde/file_tobe_save.csv'
        
    Return
    ------
    :str
        If success return htlm download location, unless None
    """
    import os
    from pathlib import Path
    from pyspark.sql import SparkSession
    from pyspark.dbutils import DBUtils
    
    spark = SparkSession.builder.appName("media_eval").getOrCreate()
    dbutils = DBUtils(spark)
    
    spark_file_path = save_file_path.spark_api()
    
    # save single csv
    (sf
     .coalesce(1)
     .write
     .format("com.databricks.spark.csv")
     .mode("overwrite")
     .option("header", "true")
     .save(spark_file_path)
    )
    
    # get first file name endswith .csv, under in result path
    files = dbutils.fs.ls(spark_file_path)
    single_csv_file_key = [file.path for file in files if file.path.endswith(".csv")][0]
    fixed_csv_file_key = (save_file_path/save_file_path.name).spark_api()
    
    # rename spark auto created csv file to target csv filename
    dbutils.fs.mv(single_csv_file_key, fixed_csv_file_key)    
    
    # HTML direct download link
    filestore_path_to_url = "/".join(save_file_path.parts[3:])
    html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url, save_file_path.name + '?o=2606104010931262')
    # print(f'HTML download link : {html_url}\n')
    
    return html_url

def save_PandasDataFrame_to_csv_FileStore(df: PandasDataFrame, 
                                          save_file_path: DBPath) -> str:
    """Save PandasDataFrame in DBFS FileStore as csv file
    and return HTML for download file to local machine.
    
    Parameter
    ---------
    df: pandas.DataFrame
         pandas DataFrame to save
        
    save_file_path: DBPath
        target file to save with extension
        ex. DBPath'/dbfs/FileStore/media/campaign_eval/01_hde/file_tobe_save.csv'
    
    Return
    ------
    : str
        If success return htlm download location, unless None
    """
    import os
    from pathlib import Path
    
    # if not existed prefix, create prefix
    parent_path = save_file_path.parent
    
    try:
        os.listdir(parent_path.file_api())
    except:
        os.makedirs(parent_path.file_api())
    
    try:
        df.to_csv(save_file_path.file_api(), index=False)
        
        #---- HTML direct download link
        filestore_path_to_url = "/".join(save_file_path.parts[3:])
        html_url = os.path.join('https://adb-2606104010931262.2.azuredatabricks.net/files', filestore_path_to_url + '?o=2606104010931262')
        # print(f'HTML download link : {html_url}\n')

        return html_url
    
    except Exception as e:
        print(e)
        return

# import datetime
# import shutil
# from openpyxl import Workbook
# from openpyxl.utils.dataframe import dataframe_to_rows

# wb = Workbook()
# ws = wb.active

# df = pd.DataFrame(
#     {
#         "campaign_name": campaign_name,
#         "week_num": week_num,
#         "TY_last_week": TY_last_week,
#         "store_universe": store_universe,
#         "cate_level": cate_level,
#     },
#     index=[0],
# )
# rows = dataframe_to_rows(df)
# for r_idx, row in enumerate(rows, 1):
#     for c_idx, value in enumerate(row, 1):
#         ws.cell(row=r_idx, column=c_idx, value=value)

# for title in results:
#     ws1 = wb.create_sheet(f"{title}")
#     df = {
#         key: results[title][key]
#         for key in list(results[title].keys())
#         if key != "survival"
#     }
#     df = pd.DataFrame(df, index=[0])
#     rows = dataframe_to_rows(df)
#     for r_idx, row in enumerate(rows, 1):
#         for c_idx, value in enumerate(row, 1):
#             ws1.cell(row=r_idx, column=c_idx, value=value)
#     ws2 = wb.create_sheet(f"{title}_survival")
#     df = pd.DataFrame(results[title]["survival"]).T.reset_index()
#     rows = dataframe_to_rows(df)
#     for r_idx, row in enumerate(rows, 1):
#         for c_idx, value in enumerate(row, 1):
#             ws2.cell(row=r_idx, column=c_idx, value=value)


# now_= datetime.datetime.now()
# timestamp_ = datetime.datetime.timestamp(now_)
# tmp_path = f'temp_{timestamp_}.xlsx' 
# wb.save(tmp_path )
# shutil.move(tmp_path, f"/dbfs/FileStore/niti/customer_matching/{campaign_name}/results.xlsx") 