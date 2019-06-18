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
    pdf = pdf.head(100)
    prize_count = pdf.groupby('Year')['Prize'].count()
    prize_count.plot(kind="bar",x='Year', y='Prize', color='green')
    plt.show()
    
    # stop the spark      
    stopSpark(spark)

if __name__ == "__main__":
    main()