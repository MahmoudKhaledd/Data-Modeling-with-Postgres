# Purpose of this project:

To help the Sparkify startup better understand their data and to be able to run analytical queries or build dashboards. Currently they have a directory of JSON files containing their datasets which makes it hard to query the data, find insights and understand which songs are people listening to.
I, a data engineer am responisble for collecting their data, build an ETL process and a pipeline to have the final form of the data in a database from which we can easily query the data and find inisghts.

# How to run the python scripts:

### create_tables.py >
open the terminal and run "python create_tables.py"
### etl.ipynb >
run each cell by pressing shift+ENTER
### etl.py >
in the terminal, type in "python etl.py"
### sql_queries.py >
contains the queries used and is imported into other files to use the queries.
### test.ipynp >
run each cell by pressing shift+ENTER

# An explainations of the files:

### create_tables.py >
connects to the database, drop and creates the projects tables, and then closes the connection
### etl.ipunb >
extracts the data from the JSON files, one by one, does the desired transformation and then loads to the database tables.
### etl.py >
does exactly what etl.ipynb does, but for the whole dataset files.
### sql_queries >
contains the queries used and is imported into other files to use the queries.
### test.ipynp >
is responsible for running tests to make sure the data is inserted correctly in each table and also does the sanity check at the end of the project.

# Database Schema

As you can see in the sparkifydb_erd.png file, we have 5 different tables following a star schema with one fact table and four dimensions tables.

## Fact table:

### Songplays > songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

##Dimenstions tables:

### users > user_id, first, and last names, gender, and level.
### songs > song_id, title, artist, year, and duration.
### artists > artist_id, name, location, longitude, and latitude.
### times > hour, day, weekday, week of the year, month, and year.

# ETL process:

In the ETL process of this project, we had our data as two main parts, songs data, and logs data, both as JSON files in two different directories.
From the songs data directory, we extracted two tables, the songs table containing song information and artists table containing artists information.
From the log data files, we have extracted 3 tables, the songplays, documenting each time a user listens to a song, the users table containing the users info, and finally the time table, for each timestamp we extract the hour, day, weekday, week of the year, and year.

