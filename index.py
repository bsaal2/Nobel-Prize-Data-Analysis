from pyspark.sql import SparkSession
import pandas as pd
import matplotlib.pyplot as plt

# Stopping the SparkSession instance
def stopSpark(spark):
    spark.stop()

def createPandaDataFrame(sdf):
    pdf = sdf.toPandas()
    # print("{0}".format(pdf.head(2)))
    return pdf

def main():
    spark = SparkSession \
            .builder \
            .appName("Nobel Prize Data Analysis") \
            .getOrCreate()

    sdf = spark.read.csv("./datasource/archive.csv", inferSchema=True, header=True)

    # Creating the pandaDataFrame from the sparkDataFrame 
    pdf = createPandaDataFrame(sdf)
    print("{0}".format(pdf.info()))
    print("Total row count : {0}".format(pdf.shape[0]))
    print("Total missing value count : {0}".format(pdf.isna()))
    sex_count = pdf["Sex"].value_counts().head(10)
    # print("{0}".format(sex_count))
    
    # stop the spark      
    stopSpark(spark)

if __name__ == "__main__":
    main()