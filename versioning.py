import os
from datetime import datetime

VERSIONED_PATH = '/user/hdfs/versioned_data/'

def save_new_version(df):
    version_name = datetime.now().strftime('%Y%m%d%H%M%S')
    version_path = os.path.join(VERSIONED_PATH, f"version_{version_name}")
    
    df.write.parquet(version_path)
    print(f"Data saved as version: {version_path}")
