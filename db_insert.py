## This script is used to insert data into the database
import os
import json
from dotenv import load_dotenv
import pandas as pd

from utils import fast_pg_insert

def read_embedding() -> dict:
    embed_path = "files/embedding"
    segment_dict = {}
    for filename in os.listdir(embed_path):
        file_path = os.path.join(embed_path, filename)
        with open(file_path, 'r') as file:
            for line in file.readlines():
                line_json = json.loads(line)
                id = line_json['custom_id']
                embedding = line_json['response']['body']['data'][0]['embedding']
                segment_dict[id] = {}
                segment_dict[id]['id'] = id
                segment_dict[id]['embedding'] = embedding
    return segment_dict

def read_documents(segment_dict) -> tuple[dict, dict]:
    doc_path = "files/documents"
    podcast_dict = {}
    for filename in os.listdir(doc_path):
        file_path = os.path.join(doc_path, filename)
        with open(file_path, 'r') as file:
            for line in file.readlines():
                line_json = json.loads(line)
                id = line_json['custom_id']
                content = line_json['body']['input']
                title = line_json['body']['metadata']['title']
                podcast_id = line_json['body']['metadata']['podcast_id']
                start_time = line_json['body']['metadata']['start_time']
                end_time = line_json['body']['metadata']['stop_time']
            #fill out podcast_dict
                if not podcast_id in podcast_dict:
                    podcast_dict[podcast_id] = {}
                    podcast_dict[podcast_id]['id'] = podcast_id
                    podcast_dict[podcast_id]['title'] = title
            #finish filling out segment_dict
                segment_dict[id]['start_time'] = start_time
                segment_dict[id]['end_time'] = end_time
                segment_dict[id]['content'] = content
                segment_dict[id]['podcast_id'] = podcast_id
    return podcast_dict, segment_dict


def make_connection():
    load_dotenv(dotenv_path='connection.env')
    return os.getenv('CONNECTION')


def insert():
    segment_dict = read_embedding()
    podcast_dict, segment_dict = read_documents(segment_dict)
    podcast = pd.DataFrame(podcast_dict.values())
    segment = pd.DataFrame(segment_dict.values())
    connection = make_connection()
    fast_pg_insert(podcast, connection, 'podcast', podcast.columns)
    fast_pg_insert(segment, connection, 'podcast_segment', segment.columns)

if __name__ == "__main__":
    insert()