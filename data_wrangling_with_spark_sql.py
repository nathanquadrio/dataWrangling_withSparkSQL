# Take care of any imports
import datetime
import os
import pyspark                          # type: ignore
import logging
import numpy as np                      # type: ignore
import pandas as pd                     # type: ignore
import matplotlib.pyplot as plt         # type: ignore

from pyspark.sql import SparkSession            # type: ignore
from pyspark.sql import Window                  # type: ignore
from pyspark.sql.functions import udf, desc, count, col, when  # type: ignore
from pyspark.sql.functions import sum as Fsum   # type: ignore
from pyspark.sql.types import IntegerType       # type: ignore


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Set the JAVA_HOME environment variable
os.environ["JAVA_HOME"] = "/opt/homebrew/opt/openjdk@11"

# Define file paths
input_path = "./data/sparkify_log_small.json"

def main():
    try:
        # Initialize Spark session
        spark = SparkSession.builder.appName("Wrangling Data").getOrCreate()
        logger.info("Spark session created.")

        # Read JSON file into DataFrame
        user_log_df = spark.read.json(input_path)
        logger.info("Data read from JSON file.")

        # user_log_df.printSchema()

        # # Create a View and Run Queries
        #
        # Create a temporary view against which one can run SQL queries
        
        user_log_df.createOrReplaceTempView("user_log_table")

        # spark.sql('''
        #         SELECT *
        #         FROM user_log_table
        #         LIMIT 2 
        #         '''
        #         ).show()
        
        # spark.sql('''
        #         SELECT COUNT(*)
        #         FROM user_log_table
        #         '''
        #         ).show()
        
        # spark.sql('''
        #         SELECT userId, firstname, page, song
        #         FROM user_log_table
        #         WHERE userId == '1046' 
        #         '''
        #         ).show()
        
        # spark.sql('''
        #         SELECT DISTINCT page
        #         FROM user_log_table
        #         ORDER BY page ASC
        #         '''
        #         ).show()
        
        # # User Defined Functions
        spark.udf.register("get_hour", lambda x: datetime.datetime.fromtimestamp(x / 1000.0).hour)
        # get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)
        
        # spark.sql('''
        #         SELECT *, get_hour(ts) AS hour
        #         FROM user_log_table
        #         LIMIT 5
        #         '''
        #         ).show()
        
        songs_in_hours_df = spark.sql('''
                    SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
                    FROM user_log_table
                    WHERE page == "NextSong"
                    GROUP BY hour
                    ORDER BY cast(hour as int) ASC
                    ''')
        
        # songs_in_hours_df.show()

        # # Converting Results to Pandas

        songs_in_hours_df = songs_in_hours_df.toPandas()
        # print(songs_in_hours_df)

        # # Question 1
        # 
        # Which page did user id ""(empty string) NOT visit?
        spark.sql('''
                SELECT DISTINCT page
                FROM user_log_table
                WHERE userid == ""
                '''
                ).show()
        
        # # Question 3
        # 
        # How many female users do we have in the data set?
        spark.sql('''
                SELECT COUNT(DISTINCT userId) AS female_user_count
                FROM user_log_table
                WHERE gender == "F"
                '''
                ).show()
        
        # # Question 4
        # 
        # How many songs were played from the most played artist?
        spark.sql('''
                SELECT artist, COUNT(artist) AS play_count
                FROM user_log_table
                GROUP BY artist
                ORDER BY play_count DESC
                LIMIT 1
                '''
                ).show()
        
        # # Question 5 (challenge)
        # 
        # How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
        
        spark.udf.register("home_flag", lambda x: 1 if x == 'Home' else 0, IntegerType())

        pages_df = spark.sql('''
                SELECT *, SUM(home_flag(page)) OVER (PARTITION BY userId ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS home_cumsum
                FROM user_log_table
                WHERE page in ('Home', 'NextSong') AND userId != ""
                ORDER BY userId, ts ASC
                '''
                )
        
        pages_df.createOrReplaceTempView("filtered_pages")

        # Count the 'NextSong' actions between each 'Home' visit and compute the average directly
        avg_next_song_count_df = spark.sql('''
                SELECT ROUND(AVG(next_song_count)) AS avg_next_song_count
                FROM (
                    SELECT userId, home_cumsum, COUNT(*) AS next_song_count
                    FROM filtered_pages
                    WHERE page = 'NextSong'
                    GROUP BY userId, home_cumsum
                )
                LIMIT 1
                '''
                ).show()


    except Exception as e:
        logger.error(f"An error occurred: {e}")
    
    finally:
        # Stop the Spark session
        spark.stop()
        logger.info("Spark session stopped.")

if __name__ == "__main__":
    main()