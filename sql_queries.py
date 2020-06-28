import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS song"
artist_table_drop = "DROP TABLE IF EXISTS artist"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS stagingevents
(               artist          VARCHAR              NULL, 
                auth            VARCHAR              NULL,  
                firstName       VARCHAR              NULL, 
                gender          CHAR(1)              NULL, 
                itemInSession   INTEGER              NULL, 
                lastName        VARCHAR              NULL, 
                length          FLOAT                NULL, 
                level           VARCHAR              NULL, 
                location        VARCHAR              NULL, 
                method          VARCHAR              NULL, 
                page            VARCHAR              NULL, 
                registration    VARCHAR              NULL, 
                sessionId       INTEGER              NOT NULL SORTKEY DISTKEY,
                song            VARCHAR              NULL, 
                status          INTEGER              NULL, 
                ts              BIGINT               NOT NULL,
                userAgent       VARCHAR              NULL, 
                userid          INTEGER              NOT NULL
)""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS stagingsongs ( 
                artist_id        VARCHAR             NOT NULL SORTKEY DISTKEY, 
                artist_lattitude FLOAT               NULL,  
                artist_location  VARCHAR             NULL,  
                artist_longitude FLOAT               NULL, 
                artist_name      VARCHAR             NULL, 
                duration         FLOAT               NULL,
                num_songs        INTEGER             NULL,
                song_id          VARCHAR             NOT NULL,
                title            VARCHAR             NULL,
                year             INTEGER             NULL)""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplay(
                songplay_id INTEGER IDENTITY(0,1)   NOT NULL SORTKEY,
                start_time  TIMESTAMP               NOT NULL, 
                user_id     VARCHAR(50)             NOT NULL DISTKEY,  
                level       VARCHAR(20)             NOT NULL, 
                song_id     VARCHAR(50)             NOT NULL, 
                artist_id   VARCHAR(50)             NOT NULL,
                session_id  VARCHAR(50)             NOT NULL,
                location    VARCHAR(100)            NULL, 
                user_agent  VARCHAR(255)            NULL)""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users(
                user_id     INTEGER                 NOT NULL SORTKEY, 
                first_name  VARCHAR(25)             NULL, 
                last_name   VARCHAR(25)             NULL, 
                gender      VARCHAR(10)             NULL, 
                level       VARCHAR(10)             NULL)
                diststyle all;""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS song(
                song_id    VARCHAR(50)             NOT NULL SORTKEY, 
                title      VARCHAR(255)            NOT NULL,
                artist_id  VARCHAR(50)             NOT NULL, 
                year       INTEGER                 NOT NULL, 
                duration   DECIMAL(10)             NOT NULL)""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artist(
                artist_id   VARCHAR(50)            NOT NULL SORTKEY,
                name        VARCHAR(255)           NULL,
                location    VARCHAR(255)           NULL,
                lattitude    DECIMAL(10)            NULL,
                longitude   DECIMAL(10)            NULL)
                diststyle all;""")


time_table_create = ("""
CREATE TABLE IF NOT EXISTS time(
                start_time  TIMESTAMP              NOT NULL SORTKEY, 
                hour        SMALLINT               NULL, 
                day         SMALLINT               NULL, 
                week        SMALLINT               NULL, 
                month       SMALLINT               NULL,  
                year        SMALLINT               NULL, 
                weekday     SMALLINT               NULL)
                diststyle all;""")

# STAGING TABLES
staging_events_copy = ("""
    copy stagingevents from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' 
    COMPUPDATE OFF
    format as JSON {log_json_path}
    timeformat as 'epochmillisecs';
""").format(data_bucket=config['S3']['LOG_DATA'], role_arn=config['IAM_ROLE']['ARN'], log_json_path=config['S3']['LOG_JSONPATH'])

staging_songs_copy = ("""
    copy staging_songs from {data_bucket}
    credentials 'aws_iam_role={role_arn}'
    region 'us-west-2' 
    COMPUPDATE OFF
    format as JSON 'auto';
""").format(data_bucket=config['S3']['SONG_DATA'], role_arn=config['IAM_ROLE']['ARN'])



user_table_insert = ("""
INSERT INTO users
    (user_id, first_name, last_name, gender, level)
    SELECT DISTINCT userid,
    firstname,
    lastname,
    gender,
    level
    FROM stagingevents
    WHERE userid IS NOT NULL
    and page = 'NextSong';
""")

song_table_insert = ("""
INSERT INTO song
    (song_id, title, artist_id, year, duration)
    SELECT DISTINCT song_id,
    title,
    artist_id,
    year,
    duration
    FROM stagingsongs
    WHERE song_id IS NOT NULL;
""")

artist_table_insert = ("""
INSERT INTO artist
(artist_id, name, location, lattitude, longitude)
    SELECT DISTINCT artist_id,
    artist_name,
    artist_location,
    artist_lattitude,
    artist_longitude
    FROM stagingsongs
    WHERE artist_id IS NOT NULL;
""")
time_table_insert = ("""
INSERT INTO time
(start_time, hour, day, week, month, year, weekday)
    SELECT DISTINCT start_time,
    EXTRACT(hour from start_time),
    EXTRACT(day from start_time),
    EXTRACT(week from start_time),
    EXTRACT(month from start_time),
    EXTRACT(year from start_time),
    EXTRACT(weekday from start_time)
    FROM songplay;
""")

songplay_table_insert = ("""
INSERT INTO songplay 
    ( start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
    SELECT TIMESTAMP 'epoch' + e.ts/1000 * INTERVAL '1 second' as start_time,
    e.userid AS user_id,
    e.level  AS level, 
    s.song_id AS song_id, 
    s.artist_id AS artist_id,
    e.sessionId AS session_id,
    e.location AS location,
    e.useragent AS user_agent
    FROM stagingevents AS e 
    join stagingsongs AS s
    on s.artist_name = e.artist
    and s.title = e.song
    WHERE e.page = 'NextSong'
    and s.artist_name is not null
    and s.title is not null;
""")






# QUERY LISTS
analytics_queries = ['select COUNT(*) AS total FROM artist','select COUNT(*) AS total FROM song','select COUNT(*) AS total FROM time','select COUNT(*) AS total FROM users','select COUNT(*) AS total FROM songplay']
create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
