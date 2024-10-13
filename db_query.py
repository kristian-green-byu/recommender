## This script is used to query the database
import os
import psycopg2
from dotenv import load_dotenv

def make_segment_query(segment_id, order):
    return f"""
        SELECT a.title as "podcast name", a.id as "segment id",
            a.content as "raw text", a.start_time as "start time",
            a.end_time as "stop time", a.distance as "embedding distance"
        FROM
            (
            SELECT b.title, c.id, c.content, c.start_time, c.end_time,
                c.embedding <->
                    (SELECT embedding FROM podcast_segment
                    WHERE id='{segment_id}') as distance
            FROM
                (SELECT * FROM podcast_segment
                    WHERE id != '{segment_id}') as c
            INNER JOIN podcast as b
            ON b.id = c.podcast_id
            ) as a
            ORDER BY a.distance {order}
            LIMIT 5;
    """

def make_segment_to_episode_query(segment_id):
    return f"""
    SELECT a.title as "Podcast title", a.distance as "Embedding distance"
    FROM
        (SELECT b.title, AVG(embedding) <->
            (SELECT embedding FROM podcast_segment
            WHERE id='{segment_id}') as distance
        FROM 
            (SELECT * FROM podcast_segment
            WHERE podcast_id !=
                (SELECT podcast_id
                FROM podcast_segment
                WHERE id = '{segment_id}')
            ) as c
        INNER JOIN podcast as b
        ON b.id = c.podcast_id
        GROUP BY b.title
        ) as a
    ORDER BY a.distance
    LIMIT 5;
    """

def execute_query6(CONNECTION):
    question = """For podcast episode id = VeH7qKZr0WI, find the five most similar
      podcast episodes. Hint: you can do a similar averaging procedure as Q5"""
    episode = "Balaji Srinivasan: How to Fix Government, Twitter, Science, and the FDA | Lex Fridman Podcast #331"
    query_q6 = """
    SELECT a.title as "Podcast title", a.dist as "Embedding distance"
    FROM
        (SELECT b.title, AVG(embedding) <->
            (SELECT AVG(embedding)
            FROM podcast_segment
            INNER JOIN podcast
            ON podcast_id = podcast.id
            WHERE podcast_id='VeH7qKZr0WI'
            GROUP BY podcast_id) as dist
        FROM 
            (SELECT * FROM podcast_segment
            WHERE podcast_id != 'VeH7qKZr0WI'
            ) as c
        INNER JOIN podcast as b
        ON b.id = c.podcast_id
        GROUP BY b.title
        ) as a
    ORDER BY a.dist
    LIMIT 5;
    """
    output_query(CONNECTION, question, query_q6, "query6", episode, "episode")

def execute_query5(CONNECTION):
    question = """For each of the following podcast segments, find the five most similar 
    podcast episodes. Hint: You can do this by averaging over the embedding vectors within a podcast episode."""
    segments_list = ["267:476", "48:511","51:56"]
    query_q5a=make_segment_to_episode_query(segments_list[0])
    output_query(CONNECTION, question, query_q5a, "query5a", segments_list[0])
    query_q5b=make_segment_to_episode_query(segments_list[1])
    output_query(CONNECTION, question, query_q5b, "query5b", segments_list[1])
    query_q5c=make_segment_to_episode_query(segments_list[2])
    output_query(CONNECTION, question, query_q5c, "query5c", segments_list[2])

def execute_query4(CONNECTION):
    question = "What are the five most similar segments to segment '51:56'"
    segment_id = '51:56'
    segment = "But what about like the fundamental physics of dark energy? Is there any understanding of what the heck it is?"
    query_q4 = make_segment_query(segment_id, "ASC")
    output_query(CONNECTION, question, query_q4, "query4", segment)

def execute_query3(CONNECTION):
    question = "What are the five most similar segments to segment '48:511'" 
    segment = """Is it is there something especially interesting and profound to you in terms 
    of our current deep learning neural network, artificial neural network approaches and the 
    whatever we do understand about the biological neural network."""
    query_q3 = make_segment_query("48:511", "ASC")
    output_query(CONNECTION, question, query_q3, "query3", segment)

def execute_query2(CONNECTION):
    question = "What are the five most dissimilar segments to segment \"267:476\"?" 
    segment = "that if we were to meet alien life at some point"
    query_q2 = make_segment_query("267:476", "DESC")
    output_query(CONNECTION, question, query_q2, "query2", segment)

def execute_query1(CONNECTION):
    question = "What are the five most similar segments to segment \"267:476\"?"
    segment = "that if we were to meet alien life at some point"
    query_q1 = make_segment_query("267:476", "ASC")
    output_query(CONNECTION, question, query_q1, "query1", segment)

def output_query(CONNECTION, question, query, name, segment, podcast_type="segment"):
    with psycopg2.connect(CONNECTION) as conn:
        cursor = conn.cursor()
        cursor.execute(query)
        responses = cursor.fetchall()
        description = [i[0] for i in cursor.description]
        

    with open(f"{name}_output.txt", 'w') as outfile:
        outfile.write(f"Question: {question}\n")
        outfile.write(f"Podcast {podcast_type}: {segment}\n")
        i = 1
        for response in responses:
            j = 0
            outfile.write(f"    {i}: ")
            while j<len(description):
                outfile.write(f"{description[j]}: {response[j]} ")
                j+=1
            outfile.write('\n')
            i+=1
def main():  
    load_dotenv(dotenv_path='connection.env')
    CONNECTION = os.getenv('CONNECTION')
    execute_query1(CONNECTION)
    execute_query2(CONNECTION)
    execute_query3(CONNECTION)
    execute_query4(CONNECTION)
    execute_query5(CONNECTION)
    execute_query6(CONNECTION)


if __name__ == "__main__":
    main()