import geopandas as gpd
import pandas as pd
import numpy as np
import plotly.express as px
import os
from pyspark.sql import SparkSession

curren_path =  os.getcwd()
root_path = os.path.dirname(curren_path)

denue_gdf = gpd.read_file(root_path + '/resources/denue_inegi_14_.shp')

def counter(gdf, gouper, column, name):
    """
    This function counts the total of posibilities in "column" 
    grouper by the "grouper".

    Parameters.
    gouper: This is the column that will help us to group the options
    column: this is the column with the options the we want to count
    name: Nome of the csv file

    Return.
    stores a csv file in the resources folder
    """

    df =  pd.DataFrame(gdf[[gouper, column]])
    my_grouper = gouper
    column2 = column

    #Create PySpark SparkSession
    spark = SparkSession.builder \
        .master("local[*]") \
        .getOrCreate()
    
    #Create PySpark DataFrame from Pandas
    spark_df = spark.createDataFrame(df) 
    spark_df.createOrReplaceTempView("table")   
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


counter(denue_gdf, 'ageb', 'nombre_act', 'stores_by_ageb')