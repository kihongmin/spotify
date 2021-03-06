# -*- coding: utf-8 -*-
import sys
sys.path.append('./libs')
import logging
import requests
import pymysql
import fb_bot
import json
import base64
import boto3
import config
import jsonpath

logger = logging.getLogger()
logger.setLevel(logging.INFO)

PAGE_TOKEN = config.facebook_token
VERIFY_TOKEN = "verify_123"


host = config.host
port = config.port
user = config.user
password = config.password
database = config.database

client_id = config.client_id
client_secret = config.client_secret

try:
    conn = pymysql.connect(host, user=user, passwd=password, db=database, port=port, use_unicode=True, charset='utf8')
    cursor = conn.cursor()
except:
    logging.error("could not connect to rds")
    sys.exit(1)

bot = fb_bot.Bot(PAGE_TOKEN)

def lambda_handler(event, context):
    # event['params'] only exists for HTTPS GET
    if 'params' in event.keys():
        if event['params']['querystring']['hub.verify_token'] == VERIFY_TOKEN:
            return int(event['params']['querystring']['hub.challenge'])
        else:
            logging.error('wrong validation token')
            raise SystemExit
    else:
        logger.info(event)
        messaging = event['entry'][0]['messaging'][0]
        user_id = messaging['sender']['id']

        logger.info(messaging)
        artist_name = messaging['message']['text']

        query = "SELECT image_url, url FROM artists WHERE name = '{}'".format(artist_name)
        cursor.execute(query)
        raw = cursor.fetchall()
        if len(raw) == 0:
            text = search_artist(cursor, artist_name)
            bot.send_text(user_id, text)
            sys.exit(0)

        image_url, url = raw[0]

        payload = {
            'template_type': 'generic',
            'elements': [
                {
                    'title': "Artist Info: '{}'".format(artist_name),
                    'image_url': image_url,
                    'subtitle': 'information',
                    'default_action': {
                        'type': 'web_url',
                        'url': url,
                        'webview_height_ratio': 'full'
                    }
                }
            ]
        }

        bot.send_attachment(user_id, "template", payload)
        artist_id = cursor.execute("select id from artists where name='{}'".format(artist_name))
        artist_id = cursor.fetchall()[0][0]

        query = '''
        SELECT y_artist
        FROM related_artists
        WHERE artist_id='{}'
        ORDER BY distance
        limit 3
        '''.format(artist_id)

        cursor.execute(query)
        neighbors = []
        for (neighbor, ) in cursor.fetchall():
            neighbors.append(neighbor)
        if neighbors == []:
            text = "sorry, we do not have related data about {}".format(artist_name)
            bot.send_text(user_id, text)
        else:
            URL = "https://api.spotify.com/v1/artists/?ids={}".format(','.join(neighbors))

            headers = get_headers(client_id,client_secret)
            r = requests.get(URL, headers=headers)
            raw = json.loads(r.text)
            neighbors = list(map(lambda x: jsonpath.jsonpath(x,'name')[0],raw['artists']))

            text = "Similar Artists : \n{}".format('\n'.join(neighbors))
            bot.send_text(user_id, text)

def get_headers(client_id, client_secret):

    endpoint = "https://accounts.spotify.com/api/token"
    encoded = base64.b64encode("{}:{}".format(client_id, client_secret).encode('utf-8')).decode('ascii')

    headers = {
        "Authorization": "Basic {}".format(encoded)
    }

    payload = {
        "grant_type": "client_credentials"
    }

    r = requests.post(endpoint, data=payload, headers=headers)

    access_token = json.loads(r.text)['access_token']

    headers = {
        "Authorization": "Bearer {}".format(access_token)
    }

    return headers


def insert_row(cursor, data, table):

    placeholders = ', '.join(['%s'] * len(data))
    columns = ', '.join(data.keys())
    key_placeholders = ', '.join(['{0}=%s'.format(k) for k in data.keys()])
    sql = "INSERT INTO %s ( %s ) VALUES ( %s ) ON DUPLICATE KEY UPDATE %s" % (table, columns, placeholders, key_placeholders)
    cursor.execute(sql, list(data.values())*2)

def invoke_lambda(fxn_name, payload, invocation_type='Event'):

    lambda_client = boto3.client('lambda')

    invoke_response = lambda_client.invoke(
        FunctionName = fxn_name,
        InvocationType = invocation_type,
        Payload = json.dumps(payload)
    )

    if invoke_response['StatusCode'] not in [200, 202, 204]:
        logging.error("ERROR: Invoking lmabda function: '{0}' failed".format(fxn_name))


    return invoke_response


def search_artist(cursor, artist_name):
    headers = get_headers(client_id, client_secret)
    ## Spotify Search API
    params = {
        "q": artist_name,
        "type": "artist",
        "limit": "1"
    }

    r = requests.get("https://api.spotify.com/v1/search", params=params, headers=headers)

    raw = json.loads(r.text)

    if raw['artists']['items'] == []:
        return "Could not find artist. Please Try Again!"

    artist = {}
    artist_raw = raw['artists']['items'][0]
    if artist_raw['name'] == params['q']:

        artist.update(
            {
                'id': artist_raw['id'],
                'name': artist_raw['name'],
                'followers': artist_raw['followers']['total'],
                'popularity': artist_raw['popularity'],
                'url': artist_raw['external_urls']['spotify'],
                'image_url': artist_raw['images'][0]['url']
            }
        )

        for i in artist_raw['genres']:
            if len(artist_raw['genres']) != 0:
                insert_row(cursor, {'artist_id': artist_raw['id'], 'genre': i}, 'artist_genres')

        insert_row(cursor, artist, 'artists')
        conn.commit()
        r = invoke_lambda('spotify-lambda', payload={'artist_id': artist_raw['id']})
        print(r)

        return "We added artist. Please try again in a second!"

    return "Could not find artist. Please Try Again!"
