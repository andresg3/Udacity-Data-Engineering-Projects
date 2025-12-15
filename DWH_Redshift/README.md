# Sparkify Redshift Warehouse (Python + AWS)

(Udacity: Data Engineering Nano Degree) | This project is part of the Udacity Data Engineer Nanodegree.

Cloud data warehouse for the Sparkify music app. Python scripts create a Redshift schema, stage raw JSON from S3, and load an analytics-ready star schema.

## 1) What this project is
- Goal: Move Sparkify’s song and event data to a Redshift warehouse for fast analytics.
- Data sources: Two S3 prefixes already populated with Udacity-provided JSON:
  - Song metadata: `s3://data.aws.bucket/data/song_data`
  - App event logs: `s3://data.aws.bucket/data/log_data` (+ JSONPath file for parsing)
- Tech: Amazon Redshift, IAM role for S3 access, Python (psycopg2) for orchestration.

## 2) High-level overview
- Infrastructure: Redshift cluster (host/db/user/pass in `dwh.cfg`) and IAM role (`S3AccessForRedshift`) with S3 read permissions.
- Schema design: Star schema optimized for song-play analytics:
  - Fact: `songplays`
  - Dimensions: `users`, `songs`, `artists`, `time`
  - Dist/Sort keys on `start_time` for time-based queries.
- Staging-first pattern:
  - Stage raw S3 JSON into `staging_events` and `staging_songs` via Redshift `COPY`.
  - Insert into final tables with lightweight transforms (join song/event, filter `page='NextSong'`, convert timestamps, derive time parts).
- Python entry points:
  - `create_tables.py` drops/creates all tables.
  - `etl.py` runs the staging `COPY` commands then inserts into the star schema.
  - `sql_queries.py` holds all DDL/DML; `dbconn.py` centralizes the Redshift connection.

## 3) Source data (raw examples)
- Song JSON (from `song_data`):
  ```json
  {"num_songs": 1, "artist_id": "ARJIE2Y1187B994AB7", "artist_latitude": null,
   "artist_longitude": null, "artist_location": "", "artist_name": "Line Renaud",
   "song_id": "SOUPIRU12A6D4FA1E1", "title": "Der Kleine Dompfaff",
   "duration": 152.92036, "year": 0}
  ```
- Log JSON (from `log_data`):
  ```json
  {"artist": null, "auth": "Logged In", "firstName": "Walter", "gender": "M",
   "itemInSession": 0, "lastName": "Frye", "length": null, "level": "free",
   "location": "San Francisco-Oakland-Hayward, CA", "method": "GET", "page": "Home",
   "registration": 1540919166796.0, "sessionId": 38, "song": null, "status": 200,
   "ts": 1541105830796, "userAgent": "Mozilla/5.0 (...Chrome/36.0...)",
   "userId": "39"}
  ```

## 4) Schema for song-play analysis
- Fact
  - `songplays`: `songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent`
- Dimensions
  - `users`: `user_id, first_name, last_name, gender, level`
  - `songs`: `song_id, title, artist_id, year, duration`
  - `artists`: `artist_id, name, location, latitude, longitude`
  - `time`: `start_time, hour, day, week, month, year, weekday`

## 5) Data flow
```
S3 (song_data JSON)         S3 (log_data JSON + JSONPath)
          |                             |
          | COPY ... JSON 'auto'        | COPY ... JSON <path>
          v                             v
   staging_songs (Redshift)      staging_events (Redshift)
               \                       /
                \ JOIN/derive/filter (Python executes SQL)
                 \____________________/
                           |
                           v
   songplays fact + users, songs, artists, time dimensions (Redshift)
```

## 6) Key scripts and snippets
- `create_tables.py`
  ```python
  from sql_queries import create_table_queries, drop_table_queries
  conn, cur = connect()
  for q in drop_table_queries:
      cur.execute(q); conn.commit()
  for q in create_table_queries:
      cur.execute(q); conn.commit()
  ```
- `sql_queries.py` (staging definitions)
  ```sql
  CREATE TABLE IF NOT EXISTS staging_events (... ts BIGINT, ...);
  CREATE TABLE IF NOT EXISTS staging_songs  (... song_id VARCHAR, ...);
  ```
- `sql_queries.py` (COPY commands)
  ```sql
  COPY staging_events
  FROM 's3://data.aws.bucket/data/log_data/'
  IAM_ROLE 'arn:aws:iam::166278591486:role/S3AccessForRedshift'
  REGION 'us-east-2'
  FORMAT AS json 's3://data.aws.bucket/data/log_json_path.json';
  ```
- `etl.py`
  ```python
  for query in copy_table_queries:
      cur.execute(query); conn.commit()
  for query in insert_table_queries:
      cur.execute(query); conn.commit()
  ```

## 7) How to run (outline)
1) Ensure `dwh.cfg` has your Redshift host/db/user/pass and the IAM role ARN with S3 read access.
2) From `DWH_Redshift/`, create the schema:
   ```bash
   python create_tables.py
   ```
3) Load data and build the star schema:
   ```bash
   python etl.py
   ```

## Notes
- Credentials in `dwh.cfg` are sample values; replace with your own secure settings.
- All transformations happen inside Redshift SQL; Python only orchestrates.
