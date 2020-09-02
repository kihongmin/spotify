import sys
import requests
import base64
import json
import logging
import pymysql
import csv
import config
import util
import boto3
import time
import util
import math

def main():
    conn, cursor = util.connect2RDS()
    athena = boto3.client('athena')

    query = """
        SELECT
         artist_id,
         AVG(danceability) AS danceability,
         AVG(energy) AS energy,
         AVG(loudness) AS loudness,
         AVG(speechiness) AS speechiness,
         AVG(acousticness) AS acousticness,
         AVG(instrumentalness) AS instrumentalness
        FROM
         top_tracks t1
        JOIN
         audio_features t2 ON t2.id = t1.id AND CAST(t1.dt AS DATE) = DATE('2020-09-02') AND CAST(t2.dt AS DATE) = DATE('2020-09-02')
        GROUP BY t1.artist_id
        LIMIT 100
    """

    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    artists = process_data(results)

    query = """
        SELECT
         MIN(danceability) AS danceability_min,
         MAX(danceability) AS danceability_max,
         MIN(energy) AS energy_min,
         MAX(energy) AS energy_max,
         MIN(loudness) AS loudness_min,
         MAX(loudness) AS loudness_max,
         MIN(speechiness) AS speechiness_min,
         MAX(speechiness) AS speechiness_max,
         ROUND(MIN(acousticness),4) AS acousticness_min,
         MAX(acousticness) AS acousticness_max,
         MIN(instrumentalness) AS instrumentalness_min,
         MAX(instrumentalness) AS instrumentalness_max
        FROM
         audio_features
    """
    r = query_athena(query, athena)
    results = get_query_result(r['QueryExecutionId'], athena)
    avgs = process_data(results)[0]

    metrics = ['danceability', 'energy', 'loudness', 'speechiness', 'acousticness', 'instrumentalness']

    for i in artists:
        for j in artists:
            dist = 0
            for k in metrics:
                x = float(i[k])
                x_norm = normalize(x, float(avgs[k+'_min']), float(avgs[k+'_max']))
                y = float(j[k])
                y_norm = normalize(y, float(avgs[k+'_min']), float(avgs[k+'_max']))
                dist += (x_norm-y_norm)**2

            dist = math.sqrt(dist) ## euclidean distance

            data = {
                'artist_id': i['artist_id'],
                'y_artist': j['artist_id'],
                'distance': dist
            }

            util.insert_row(cursor, data, 'related_artists')


    conn.commit()
    cursor.close()

def query_athena(query, athena):
    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            'Database': 'mydatabase'
        },
        ResultConfiguration={
            'OutputLocation': "s3://athena-kihong-tables/repair/",
            'EncryptionConfiguration': {
                'EncryptionOption': 'SSE_S3'
            }
        }
    )
    return response

def get_query_result(query_id, athena):

    response = athena.get_query_execution(
        QueryExecutionId=str(query_id)
    )
    while response['QueryExecution']['Status']['State'] != 'SUCCEEDED':
        if response['QueryExecution']['Status']['State'] == 'FAILED':
            logging.error('QUERY FAILED')
            break
        time.sleep(5)
        response = athena.get_query_execution(
            QueryExecutionId=str(query_id)
        )

    response = athena.get_query_results(
        QueryExecutionId=str(query_id),
        MaxResults=1000
    )

    return response
def process_data(results):

    columns = [col['Label'] for col in results['ResultSet']['ResultSetMetadata']['ColumnInfo']]

    listed_results = []
    for res in results['ResultSet']['Rows'][1:]:
        values = []
        for field in res['Data']:
            try:
                values.append(list(field.values())[0])
            except:
                values.append(list(' '))
        listed_results.append(dict(zip(columns, values)))

    return listed_results

def normalize(x, x_min, x_max):
    return (x-x_min) / (x_max-x_min)


if __name__ == '__main__':
    main()
