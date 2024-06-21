# Sparkify User Log Analysis

This project analyzes user activity logs from the Sparkify music streaming app using PySpark. The goal is to extract insights and answer specific questions about user behavior. The script reads JSON logs, processes the data, and performs various SQL queries to provide meaningful statistics.

## Project Name

**Sparkify User Log Analysis**

## Requirements

- Python 3.x
- PySpark
- Java Development Kit (JDK) 11
- Additional Python libraries: `numpy`, `pandas`, `matplotlib`

## Setup

1. **Install Python Libraries**:
   Ensure the required Python libraries are installed. You can install them using `pip`:
   ```bash
   pip install pyspark numpy pandas matplotlib
   ```

2. **Set JAVA_HOME**:
   Set the `JAVA_HOME` environment variable to the path of JDK 11. For example:
   ```bash
   export JAVA_HOME="/path/to/jdk-11"
   ```

3. **Data File**:
   Place your `sparkify_log_small.json` file in the `./data/` directory or update the `input_path` variable in the script to point to the correct location.

## Script Overview

The script performs the following steps:

1. **Imports and Configurations**:
   - Import necessary libraries and modules.
   - Configure logging to capture the script's progress and errors.
   - Set the `JAVA_HOME` environment variable.

2. **Initialize Spark Session**:
   - Create a Spark session named "Wrangling Data".

3. **Read Data**:
   - Read the JSON log file into a PySpark DataFrame.

4. **Create Temporary View**:
   - Create a temporary view from the DataFrame to run SQL queries against.

5. **Define User-Defined Functions (UDFs)**:
   - Register UDFs to extract the hour from timestamps and to flag 'Home' pages.

6. **Run SQL Queries**:
   - Various SQL queries to answer specific questions:
     1. **Which page did user id "" (empty string) NOT visit?**
        ```sql
        SELECT DISTINCT page
        FROM user_log_table
        WHERE userid == ""
        ```

     2. **How many female users are in the data set?**
        ```sql
        SELECT COUNT(DISTINCT userId) AS female_user_count
        FROM user_log_table
        WHERE gender == "F"
        ```

     3. **How many songs were played from the most played artist?**
        ```sql
        SELECT artist, COUNT(artist) AS play_count
        FROM user_log_table
        GROUP BY artist
        ORDER BY play_count DESC
        LIMIT 1
        ```

     4. **How many songs do users listen to on average between visiting our home page?**
        - Filter data for 'Home' and 'NextSong' pages.
        - Calculate cumulative sum to identify segments between 'Home' visits.
        - Count 'NextSong' actions and compute the average:
        ```sql
        SELECT *, SUM(home_flag(page)) OVER (PARTITION BY userId ORDER BY ts ASC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS home_cumsum
        FROM user_log_table
        WHERE page in ('Home', 'NextSong') AND userId != ""
        ORDER BY userId, ts ASC
        ```

        ```sql
        SELECT ROUND(AVG(next_song_count)) AS avg_next_song_count
        FROM (
            SELECT userId, home_cumsum, COUNT(*) AS next_song_count
            FROM filtered_pages
            WHERE page = 'NextSong'
            GROUP BY userId, home_cumsum
        )
        LIMIT 1
        ```

7. **Exception Handling**:
   - Capture and log any exceptions that occur during execution.

8. **Stop Spark Session**:
   - Stop the Spark session to free up resources.

## Running the Script

1. Ensure you have set up the prerequisites.
2. Run the script:
   ```bash
   python data_wrangling_with_Spark_SQL.py
   ```
