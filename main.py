from LearnSqlToHdfsCollector import LearnSqlToHdfsCollector
import os

base_dir = os.path.dirname(os.path.abspath(__file__))

# Add these lines to define the local folder paths
idealista_path = os.path.join(base_dir, 'data', 'idealista')

LOCAL_FOLDERS = [idealista_path]


HDFS_URL = 'http://10.4.41.48:9870/'
hdfs_path = '/user/bdm/'

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    # Add these lines to copy local folders to HDFS
    learn_sql_to_hdfs_collector = LearnSqlToHdfsCollector(hdfs_url=HDFS_URL)
    local_folders = LOCAL_FOLDERS
    learn_sql_to_hdfs_collector.copy_to_hdfs(local_folders)