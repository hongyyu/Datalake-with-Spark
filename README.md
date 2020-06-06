## Introduction
The music streaming startup, Sparkify, is currently growing and want to move to data lake on AWS. The purpose of this project is to build an ETL pipeline to process data from `S3` on AWS and generate appropriate tables for further `OLAP (Online Analytical Processing)` of teams. Storing processed files at S3
could help the company to utilize those data more efficient and even more scalable.

## Source Data
There are mainly two sources `SONG_DATA` AND `LOG_DATA` with Amazon S3 URL
`s3://udacity-dend/song_data` and `s3://udacity-dend/log_data` respectively. Both log data and song data
are saved in `JSON` format, and we are going to use `sparkContext (sc)` to extract them from S3. Saving data
as dataframe using spark, we could customize types of each columns, or let spark to infer the appropriate 
schemas of these files.

## Database Schema
Creating and designing the database with star schema which is usually clear for business team to analyze and generate insights. There are 5 tables in total (one fact table and four dimension tables) showing below:
### Fact Table
- songplays
    * songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent
### Dimension Tables
- users
    * user_id, first_name, last_name, gender, level
- songs
    * song_id, title, artist_id, year, duration
- artists
    * artist_id, name, location, lattitude, longitude
- time
    * start_time, hour, day, week, month, year, weekday

## How to Run
### Set appropriate AWS key and S3 output
"""
etl.py
output_data = <your S3 bucket path>
"""
"""
dl.cfg
[AWS]
AWS_ACCESS_KEY_ID = <your aws key>
AWS_SECRET_ACCESS_KEY = <your aws secret>
"""
### Steps
1. Direct to the correct location where this Data Lake Project saved in the terminal
with command like `cd`
2. Update the `AWS` key and `S3` bucket path on configure file dl.cfg and etl.py seperately
3. Run `Python etl.py` in the terminal and check status.
4. After steps above, it is time to do any data analysis parts.

## Data Analysis
ETL pipline help business users more easily to understand information in the database and then
transform into insights to potential stakeholders ans customers. Instead of data warehouse, it is not 
necessary to process and store dimention and fact tables. `Schema-on-Read` is a more general way to 
deal with data since there are many unstaructed or semi-structured data like text, paragraph which could be
used for researchers and data scientists.

Please find the `Ad-hoc` queries and visualizations in the `jupyter notebook`.



