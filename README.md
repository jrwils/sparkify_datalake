# Sparkify Analytics
## Using Spark and EMR on AWS
## Project Purpose
This project, developed as part of the [Udacity Data Engineering Nanodegree](https://www.udacity.com/course/data-engineer-nanodegree--nd027), was developed to help the fictional company Sparkify organize their data for analytical purposes using Spark.  Sparkify would like to analyze user listening habits on their platform and is especially interested in finding out which songs their users are listening to.

### Song Files
Song files are nested in the project under the `song_data` directory in S3. Each file contains a single .json object. For example:

    {
        "num_songs": 1,
        "artist_id": "ARJIE2Y1187B994AB7",
        "artist_latitude": null,
        "artist_longitude": null,
        "artist_location": "",
        "artist_name": "Line Renaud",
        "song_id": "SOUPIRU12A6D4FA1E1",
        "title": "Der Kleine Dompfaff",
        "duration": 152.92036,
        "year": 0
    }

### Event Files
Events files consist of logs that are nested in the `log_data` directory. Each file contains .json objects on individual lines that represent a user action, along with information about the song played. For example:

    {
        "artist": "Des'ree",
        "auth": "Logged In",
        "firstName": "Kaylee",
        "gender": "F",
        "itemInSession": 1,
        "lastName": "Summers",
        "length": 246.30812,
        "level": "free",
        "location": "Phoenix-Mesa-Scottsdale, AZ",
        "method": "PUT",
        "page": "NextSong",
        "registration": 1540344794796.0,
        "sessionId": 139,
        "song": "You Gotta Be",
        "status": 200,
        "ts": 1541106106796,
        "userAgent": "\"Mozilla/5.0 (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.153 Safari/537.36\"",
        "userId": "8"
    }

The records that we care about in the context of this project are where `page` equals `NextSong`, as those are the records that represent a song being streamed by a user.

### Schema Design
Since we are using Spark, we will implement a 'schema-on-read' process to define the structure. The design of the output files will utilize a fact and dimension star schema, broken up into the following 5 tables: `artists`, `songs`, `songplays`, `time` and `users`. The fact table in this design is the `songplays` table, which represents the action of a user selecting a song to play. The other 4 tables are dimension tables that represent details about the song play action, including information about the song, artist, time of selection and the user who selected it.

### Setting up S3 storage

Upload the data `song_data` and `log_data` files to an S3 bucket of your choice, and make sure that the `main()` function in `etl.py` reflects your storage directories. Also, make sure the S3 bucket is accessible using an AWS Access Key and Secret, which should be pasted into `dl.cfg`.


### Running the Process

To run the Job on an AWS Spark Cluster, do the following:

1. SSH into the master node of the cluster.
2. SCP the `etl.py` and `dl.cfg` files to the `/home/hadoop` directory on the master node.
3. Run the following command in the `/home/hadoop` directory:

    `spark-submit etl.py`

4. Once the process has been run, the `output` directory configured in S3 will populated with .parquet files for each table in the schema.