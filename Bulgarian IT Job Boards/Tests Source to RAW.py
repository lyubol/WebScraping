# Databricks notebook source
import unittest
from datetime import date

# COMMAND ----------

class TestSourceRAW(unittest.TestCase):
#     today_file_path = f"/mnt/adlslirkov/it-job-boards/DEV.bg/raw/posts/{date.today().year}/{date.today().month}/{date.today().day}/posts-{date.today()}.csv"
    today_file_path = f"/mnt/adlslirkov/it-job-boards/DEV.bg/raw/posts/{date.today().year}/{date.today().month}/{date.today().day - 1}/posts-2022-08-04.csv"
    
    def test_rawFileExists(self):
        message = "Location does not exists!"
        self.assertIsNotNone(dbutils.fs.ls(TestSourceRAW.today_file_path), message)
            
    def test_rawFileNotEmpty(self):
        df_test_posts = spark.read.format("csv").load(f"/mnt/adlslirkov/it-job-boards/DEV.bg/raw/posts/{date.today().year}/{date.today().month}/{date.today().day - 1}/posts-2022-08-04.csv")
        rows_count = df_test_posts.count()
        self.assertGreater(rows_count, 0)

        
# if __name__ == '__main__':
#     unittest.main(argv=['first-arg-is-ignored'], exit=False)

suite = unittest.TestLoader().loadTestsFromTestCase(TestSourceRAW)
runner = unittest.TextTestRunner(verbosity=2)
runner.run(suite)
