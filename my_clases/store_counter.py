import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.express as px
import os
from pyspark.sql import SparkSession

curren_path =  os.getcwd()
root_path = os.path.dirname(curren_path)
my_path_file = root_path + '/resources/denue_slim.csv'


def counter(path_file, grouper, column, name):
    """
    This function counts the total of posibilities in "column" 
    grouper by the "grouper".

    Parameters.
    path_file: This is the path of the csv  with the information 
    grouper: This is the column that will help us to group the options
    column: this is the column with the options the we want to count
    name: Nome of the csv file

    Return.
    stores a csv file in the resources folder
    """
    #Create PySpark SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .getOrCreate()
    
    df = spark.read.csv(path_file, header=True)
    my_grouper = grouper
    column2 = column

    #Create PySpark DataFrame from Pandas
    #spark_df = spark.createDataFrame(df) 
    df.createOrReplaceTempView("table")   
    df2 = spark.sql(
        ''' 
        SELECT {0}, COUNT({1}) as total  
        FROM table
        GROUP BY {0}
        order by {0}
        '''.format(my_grouper, column2))
    df2.collect()
    df2.toPandas().to_csv(root_path + '/resources/' + name + '.csv', index=False)
    return()


counter(path_file, 'ageb', 'nombre_act', 'stores_by_ageb')