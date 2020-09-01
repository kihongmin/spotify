import sys
import requests
import base64
import json
import logging
import pymysql
import jsonpath
import csv
import config
import util
import pandas as pd
import datetime
import boto3

def toS3():
    conn, cursor = util.connect2RDS()
    headers = util.get_headers()

    cursor.execute("SELECT id FROM artists LIMIT 10")

    top_track_keys = {
        'id':'id',
        'name':'name',
        'popularity':'popularity',
        'external_urls':'external_urls.spotify'
    }

    top_tracks = []
    for(id, ) in cursor.fetchall():
        URL = "https://api.spotify.com/v1/artists/{}/top-tracks".format(id)
        params = {
            'country':'US'
        }
        r = requests.get(URL, params = params, headers = headers)
        raw = json.loads(r.text)
        for i in raw['tracks']:
            top_track = {}
            for k, v in top_track_keys.items():
                top_track.update({k:jsonpath.jsonpath(i,v)[0]})
                top_track.update({'artist_id':id})
                top_tracks.append(top_track)

    top_tracks = pd.DataFrame(top_tracks)
    top_tracks.to_parquet('top-tracks.parquet',engine='pyarrow',compression='snappy')

    track_ids = [top_tracks.loc[i,'id'] for i in top_tracks.index]

    dt = datetime.datetime.utcnow().strftime("%Y-%m-%d")

    s3 = boto3.resource('s3')
    object = s3.Object('kihong-spotify-lambda','top-tracks/dt={}/top-tracks.parquet'.format(dt))
    data = open('top-tracks.parquet','rb')
    object.put(Body=data)

    tracks_batch = [track_ids[i:i+100] for i in range(0,len(track_ids),100)]

    audio_features = []
    for i in tracks_batch:
        ids = ','.join(i)
        URL = "https://api.spotify.com/v1/audio-features/?ids={}".format(ids)

        r = requests.get(URL,headers=headers)
        raw = json.loads(r.text)

        audio_features.extend(raw['audio_features'])

    audio_features = pd.DataFrame(audio_features)
    audio_features.to_parquet('audio-features.parquet',engine='pyarrow',compression='snappy')

    object = s3.Object('kihong-spotify-lambda', 'audio-features/dt={}/top-tracks.parquet'.format(dt))
    data = open('audio-features.parquet', 'rb')
    object.put(Body=data)

if __name__=='__main__':
    toS3()
