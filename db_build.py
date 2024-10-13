## This script is used to create the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv(dotenv_path='connection.env')

CONNECTION = os.getenv('CONNECTION')

# need to run this to enable vector data type
CREATE_EXTENSION = "CREATE EXTENSION IF NOT EXISTS vector"

# TODO: Add create table statement
CREATE_PODCAST_TABLE = """
CREATE TABLE podcast(
    id VARCHAR(30) PRIMARY KEY,
    title TEXT NOT NULL
);
"""
# TODO: Add create table statement
CREATE_SEGMENT_TABLE = """
CREATE TABLE podcast_segment(
    id VARCHAR(30) PRIMARY KEY,
    start_time FLOAT NOT NULL,
    end_time FLOAT NOT NULL,
    content TEXT NOT NULL,
    embedding vector(128) NOT NULL,
    podcast_id VARCHAR(30) NOT NULL,
    FOREIGN KEY(podcast_id) REFERENCES podcast(id)
        ON UPDATE RESTRICT
        ON DELETE CASCADE
);
"""

conn = psycopg2.connect(CONNECTION)
cursor = conn.cursor()

cursor.execute(CREATE_EXTENSION)
cursor.execute(CREATE_PODCAST_TABLE)
cursor.execute(CREATE_SEGMENT_TABLE)

conn.commit()
conn.close()


