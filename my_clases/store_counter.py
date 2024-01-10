import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.express as px
import os
from pyspark.sql import SparkSession

class Counter:
    """
    This class has a function that counts the total of posibilities in the " target_column" 
    grouped by the "grouper".

    Parameters.
    path_file: This is the path of the csv  with the information 
    grouper: This is the column that will help us to group the options
    target_column: this is the column with the options the we want to count
    path_of_results: This is the path where de results is going to be saved
    name_file: Name of the csv file without extension

    Return.
    stores a csv file in the 'path_of_results' folder
    """

    def __init__(self, original_path_file, grouper, target_column, path_of_results, name_file):
        self.original_path_file = original_path_file
        self.grouper = grouper
        self.target_column = target_column
        self.path_of_results = path_of_results
        self.name_file = name_file

    def counter(self):
    
        #Create PySpark SparkSession
        spark = SparkSession.builder \
            .master("local[*]") \
            .getOrCreate()
        
        df = spark.read.csv(self.original_path_file, header=True)
        #my_grouper = self.grouper
        #column2 = self.target_column

        #Create PySpark DataFrame from Pandas
        #spark_df = spark.createDataFrame(df) 
        df.createOrReplaceTempView("table")   
        df2 = spark.sql(
            ''' 
            SELECT {0}, COUNT({1}) as total  
            FROM table
            GROUP BY {0}
            order by {0}
            '''.format(self.grouper, self.target_column))
        df2.collect()
        df2.toPandas().to_csv(self.path_of_results + self.name_file + '.csv', index=False)
        return(self.path_of_results + self.name_file + '.csv')



curren_path =  os.getcwd()
root_path = os.path.dirname(curren_path)
my_path_file = root_path + '/resources/denue_slim.csv'
my_resuources = root_path + '/resources/'

contador = Counter(my_path_file, 'ageb', 'nombre_act', my_resuources, 'roles_by_ageb')
print(contador.counter())



