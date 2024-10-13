## This script is used to drop the tables in the database

import os
from dotenv import load_dotenv
import psycopg2

load_dotenv(dotenv_path='connection.env')

CONNECTION = os.getenv('CONNECTION')

DROP_TABLE = "DROP TABLE podcast, podcast_segment"


with psycopg2.connect(CONNECTION) as conn:
    cursor = conn.cursor()
    cursor.execute(DROP_TABLE)
