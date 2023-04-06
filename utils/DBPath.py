from pathlib import Path as _Path_, _windows_flavour, _posix_flavour
import os

class DBPath(_Path_):
    
    _flavour = _windows_flavour if os.name == 'nt' else _posix_flavour
    
    def __init__(self, input_file):
        super().__init__()
        pass
        
    def __repr__(self):
        return f"DBPath class : {self.as_posix()}"
    
    def file_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("/dbfs"+rm_first_5_str)
    
    def spark_api(self):
        rm_first_5_str = str(self.as_posix())[5:]
        return str("dbfs:"+rm_first_5_str)