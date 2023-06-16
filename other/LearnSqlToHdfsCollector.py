from hdfs import InsecureClient
import os
import datetime


class LearnSqlToHdfsCollector:
    def __init__(self, hdfs_url: str, hdfs_directory: str):
        self.hdfs_directory = hdfs_directory
        self.hdfs_client = InsecureClient(hdfs_url, user='bdm')

    def copy_to_hdfs(self, local_folders: list):
        if self.hdfs_client.content(self.hdfs_directory, strict=False) is None:
            self.hdfs_client.makedirs(self.hdfs_directory, permission=777)

        for local_folder in local_folders:
            folder_name = os.path.basename(local_folder)
            hdfs_folder_path = self.hdfs_directory

            # Create HDFS directory
            self.hdfs_client.makedirs(hdfs_folder_path, permission=777)

            # Copy local files to HDFS
            for root, dirs, files in os.walk(local_folder):
                current_extraction = datetime.datetime.now().strftime(
                    '%Y_%m_%d_%H_%M_%S')
                for file in files:
                    local_path = os.path.join(root, file)
                    file_name, ext = os.path.splitext(file)
                    hdfs_path = hdfs_folder_path + '/' + current_extraction + '/' + file_name + '_' + current_extraction + ext
                    with open(local_path, 'rb') as f:
                        self.hdfs_client.write(hdfs_path, f, overwrite=True)


