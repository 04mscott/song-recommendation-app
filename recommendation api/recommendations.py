from sqlalchemy import create_engine
from collections import defaultdict
from dotenv import load_dotenv
from requests import post, get
import mysql.connector
import pandas as pd
import numpy as np
import base64
import json
import time
import os

load_dotenv()

client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')
db_host = os.getenv("DB_HOST")
db_port = os.getenv("DB_PORT")
db_user = os.getenv("DB_USER")
db_password = os.getenv("DB_PASSWORD")
db_name = os.getenv("DB_NAME")
youtube_key = os.getenv('YOUTUBE_KEY')


def get_auth_header(token):
    return {'Authorization': 'Bearer ' + token}
    
def connect_to_db():
    db_connection = mysql.connector.connect(
        host=db_host,
        user=db_user,
        password=db_password,
        database=db_name
    )
    return db_connection.cursor()
    
# Returns 7 pandas dataframes to be added to database
# Table 1: Users
# Table 2: Songs
# Table 3: Artists
# Table 4: Artist Genres
# Table 5: Artist <-> Song interactions
# Table 6: User <-> Song Interactions
# Table 7: User <-> Artist Interactions
def get_all_tables(token):
    headers = get_auth_header(token)
    result = {
        'users': get_user_info(token),
        'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
        'artists': pd.DataFrame(columns=['artist_id', 'name']),
        'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
        'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
        'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
        'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
    }
    start_time = time.time()
    user_id = result['users']['user_id'].iloc[0]
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')

    start_time = time.time()
    result = get_top(token, headers, user_id, result)
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')

    start_time = time.time()
    result = get_all_saved_tracks(token, headers, user_id, result)
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')

    start_time = time.time()
    result = get_followed_artists(token, headers, user_id, result)
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')

    start_time = time.time()
    result = get_all_playlist_tracks(token, headers, user_id, result)
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')


    for key in result:
        result[key] = result[key].drop_duplicates()
        
    start_time = time.time()
    result = get_genres(token, headers, user_id, result)
    end_time = time.time()
    print(f'Completed in {end_time - start_time} seconds\n')

    return result

def get_user_info(token):
    print('---------------------------------------------------')
    print('-----------------Getting User Data-----------------')
    print('---------------------------------------------------')

    url = 'https://api.spotify.com/v1/me'
    headers = get_auth_header(token)
    result = get(url=url, headers=headers)
    json_result = json.loads(result.content)
    return pd.DataFrame.from_dict({
        'user_id': [json_result['id']], 
        'email': [json_result['email']], 
        'profile_img': [json_result['images'][0]['url'] if json_result['images'] != [] else None]
    })

def get_top(token, headers, user_id, result):
    print('-----------------------------------------------------------------')
    print('-----------------Getting Top Tracks/Artists Data-----------------')
    print('-----------------------------------------------------------------')
    
    url = 'https://api.spotify.com/v1/me/top/tracks'
    params = {
        'limit': 50,
        'offset': 0
    }
    response = get(url=url, headers=headers, params=params)
    json_response = json.loads(response.content)

    songs_list = []
    artists_list = []
    user_song_interactions_list = []
    song_artist_interaction_list = []
    for item in json_response['items']:
        songs_list.append([item['id'], item['name'], item['album']['images'][0]['url'], None]) # Add song to songs list
        user_song_interactions_list.append([user_id, item['id'], False, True, False]) # Add User Song interaction
            
        for artist in item['artists']:
            artists_list.append([artist['id'], artist['name']]) # Add Artist(s) to artists list
            song_artist_interaction_list.append([item['id'], artist['id']]) # Add Song Artist Interaction

    result['songs'] = pd.concat([result['songs'], pd.DataFrame(data=songs_list, columns=result['songs'].columns)])
    result['artists'] = pd.concat([result['artists'], pd.DataFrame(data=artists_list, columns=result['artists'].columns)])
    result['user_song_interactions'] = pd.concat([result['user_song_interactions'], pd.DataFrame(data=user_song_interactions_list, columns=result['user_song_interactions'].columns)])
    result['song_artist_interactions'] = pd.concat([result['song_artist_interactions'], pd.DataFrame(data=song_artist_interaction_list, columns=result['song_artist_interactions'].columns)])
        
    # Top Artists
    url = 'https://api.spotify.com/v1/me/top/artists'
    params = {
        'limit': 50,
        'offset': 0
    }
    response = get(url=url, headers=headers, params=params)
    json_response = json.loads(response.content)

    artists_list = []
    user_artist_interaction_list = []
    for item in json_response['items']:
        artists_list.append([item['id'], item['name']]) # Add artist to artists list
        user_artist_interaction_list.append([user_id, item['id'], False, True]) # Add User Artist interaction

    result['artists'] = pd.concat([result['artists'], pd.DataFrame(data=artists_list, columns=result['artists'].columns)])
    result['user_artist_interactions'] = pd.concat([result['user_artist_interactions'], pd.DataFrame(data=user_artist_interaction_list, columns=result['user_artist_interactions'].columns)])
    return result

def get_all_saved_tracks(token, headers, user_id, result):
    print('------------------------------------------------------')
    print('-----------------Getting Saved Tracks-----------------')
    print('------------------------------------------------------')

    url = 'https://api.spotify.com/v1/me/tracks'
    count = 0

    songs_list = []
    artists_list = []
    user_song_interactions_list = []
    song_artist_interaction_list = []

    while True:
        params = {
            'limit': 50,
            'offset': 50 * count
        }

        response = get(url=url, headers=headers, params=params)
        json_response = json.loads(response.content)

        if 'items' not in json_response or not json_response['items']:
            break

        for item in json_response['items']:
            track = item['track']
            songs_list.append([track['id'], track['name'], track['album']['images'][0]['url'], None]) # Add Song

            # Add User Song Interactions (checking to see if already in table)
            matching_rows = result['user_song_interactions'][
                (result['user_song_interactions']['user_id'] == user_id) & 
                (result['user_song_interactions']['song_id'] == track['id'])
            ]

            if not matching_rows.empty:
                index = matching_rows.index[0]
                result['user_song_interactions'].at[index, 'saved'] = True 
            else:
                user_song_interactions_list.append([user_id, track['id'], True, False, False])
            
            for artist in track['artists']:
                artists_list.append([artist['id'], artist['name']]) # Add Artist(s)
                song_artist_interaction_list.append([track['id'], artist['id']]) # Add Song Artist Interactions

        count += 1
    result['songs'] = pd.concat([result['songs'], pd.DataFrame(data=songs_list, columns=result['songs'].columns)])
    result['artists'] = pd.concat([result['artists'], pd.DataFrame(data=artists_list, columns=result['artists'].columns)])
    result['user_song_interactions'] = pd.concat([result['user_song_interactions'], pd.DataFrame(data=user_song_interactions_list, columns=result['user_song_interactions'].columns)])
    result['song_artist_interactions'] = pd.concat([result['song_artist_interactions'], pd.DataFrame(data=song_artist_interaction_list, columns=result['song_artist_interactions'].columns)])
    return result

def get_all_playlist_tracks(token, headers, user_id, result):
    print('---------------------------------------------------')
    print('-----------------Getting Playlists-----------------')
    print('---------------------------------------------------')

    url = f'https://api.spotify.com/v1/users/{user_id}/playlists'
    count = 0
    playlist_count = 0

    while True:
        params = {
            'limit': 50,
            'offset': 50 * count
        }
        response = get(url=url, headers=headers, params=params)

        if response.status_code == 200:
            json_response = json.loads(response.content)
        elif response.status_code == 429:
            retry_after = int(response.headers.get("Retry-After", 5))  # Default to 5s if missing
            print(f"Rate limited! Retrying after {retry_after} seconds...")
            time.sleep(retry_after)
        elif not response.content.strip():  # Ensure it's not empty
            print("Warning: Empty response from API")
            break
        else:
            print(f"Error: {response.status_code}, {response.text}")
            break

        if 'items' not in json_response or not json_response['items']:
            break

        for i, item in enumerate(json_response['items']):
            playlist_count += 1
            playlist_response = get(url=item['href'], headers=headers)
            json_playlist_response = json.loads(playlist_response.content)
            total_tracks = json_playlist_response['tracks']['total']
            tracks_url = json_playlist_response['tracks']['href']

            track_count = 0
            loops = 0
            while track_count < total_tracks:
                params = {
                    'limit': 100,
                    'offset': 100 * loops
                }
                tracks_response = get(url=tracks_url, headers=headers, params=params)
                json_tracks_response = json.loads(tracks_response.content)
                    
                if 'items' not in json_tracks_response or not json_tracks_response['items']:
                    break

                songs_list = []
                artists_list = []
                user_song_interactions_list = []
                song_artist_interactions_list = []

                for item in json_tracks_response['items']:
                    if track_count >= total_tracks:
                        break
                    if item.get('track') and item['track'].get('name') and item['track'].get('artists'):
                        track = item['track']
                        songs_list.append([track['id'], track['name'], track['album']['images'][0]['url'], None]) # Add song to df

                        # Add User Song Interactions (checking to see if already in table)
                        matching_rows = result['user_song_interactions'][
                            (result['user_song_interactions']['user_id'] == user_id) & 
                            (result['user_song_interactions']['song_id'] == track['id'])
                        ]

                        if not matching_rows.empty:
                            index = matching_rows.index[0]
                            result['user_song_interactions'].at[index, 'playlist'] = True 
                        else:
                            user_song_interactions_list.append([user_id, track['id'], False, False, True])

                        # Add artists to table
                        for artist in track['artists']:
                            artists_list.append([artist['id'], artist['name']]) # Add Artist(s)
                            song_artist_interactions_list.append([track['id'], artist['id']]) # Add Song Artist Interactions

                    track_count += 1
                loops += 1   

                result['songs'] = pd.concat([result['songs'], pd.DataFrame(data=songs_list, columns=result['songs'].columns)])
                result['artists'] = pd.concat([result['artists'], pd.DataFrame(data=artists_list, columns=result['artists'].columns)])
                result['user_song_interactions'] = pd.concat([result['user_song_interactions'], pd.DataFrame(data=user_song_interactions_list, columns=result['user_song_interactions'].columns)])
                result['song_artist_interactions'] = pd.concat([result['song_artist_interactions'], pd.DataFrame(data=song_artist_interactions_list, columns=result['song_artist_interactions'].columns)])    

        if(playlist_count >= json_response['total']):
            break

        count += 1
    return result

def get_followed_artists(token, headers, user_id, result):
    print('----------------------------------------------------------')
    print('-----------------Getting Followed Artists-----------------')
    print('----------------------------------------------------------')

    url = 'https://api.spotify.com/v1/me/following?type=artist&limit=50'

    user_artist_interactions_list = []
    while True:
        response = get(url=url, headers=headers)

        json_response = json.loads(response.content)

        if 'artists' not in json_response or not json_response['artists']:
            break

        for artist in json_response['artists']:
            if 'item' not in artist or not artist['items']:
                break
            for item in artist['items']:
                matching_rows = result['user_artist_interactions'][
                    (result['user_artist_interactions']['user_id'] == user_id) & 
                    (result['user_artist_interactions']['artist_id'] == item['id'])
                ]

                if not matching_rows.empty:
                    index = matching_rows.index[0]
                    result['user_artist_interactions'].at[index, 'follows'] = True 
                else:
                    user_artist_interactions_list.append([user_id, item['id'], True, False])
                after = item['id']

        if json_response['artists']['next']:
            url = json_response['artists']['next']
        else:
            break

    result['user_artist_interactions'] = pd.concat([result['user_artist_interactions'], pd.DataFrame(data=user_artist_interactions_list, columns=result['user_artist_interactions'])])
    return result

def get_genres(token, headers, user_id, result):
    print('----------------------------------------------------')
    print('-----------------Getting Genre Data-----------------')
    print('----------------------------------------------------')

    url = 'https://api.spotify.com/v1/artists'
    artists = result['artists']
    artist_ids = artists['artist_id'].tolist()
    artist_genres = result['artist_genres']
    end = 50

    artist_genres_list = []
    while end < len(artist_ids) or abs(len(artist_ids) - end) < 50:
        params = {
            'ids': ','.join(artist_ids[end-50:end])
        }
        response = get(url=url, headers=headers, params=params)
        json_response = json.loads(response.content)

        for artist in json_response['artists']:
            artist_id = artist['id']
            if artist.get('genres'):
                for genre in artist['genres']:
                    artist_genres_list.append([artist_id, genre])
        end += 50
    result['artist_genres'] = pd.concat([result['artist_genres'], pd.DataFrame(data=artist_genres_list, columns=result['artist_genres'].columns)])
    return result

def add_df_to_db(dataframes, prompts=False):
    for df in dataframes:
        if prompts:
            answer = input(f'Add {df} to Database? [y/n]')
        connection_string = f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}'
        engine = create_engine(connection_string)
        dataframes[df].to_sql(df, con=engine, if_exists='append', index=False)
        print('Data inserted successfully')

def get_preview(song_name, artist_name):
    try:
        url = 'https://www.googleapis.com/youtube/v3/search'
        query = f'{song_name} {artist_name} official audio'
        params = {
            'part': 'snippet',
            'q': query,
            'key': youtube_key,
            'maxResults': 1,
            'type': 'video'
        }

        response = get(url=url, params=params)
        json_response = response.json()

        if 'items' in json_response and json_response['items']:
            video_id = json_response['items'][0]['id']['videoId']
            return f'https://www.youtube.com/watch?v={video_id}'
    except Exception as e:
        print(f'YouTube API error: {e}')

    return None

if __name__=='__main__':
    USER = False
    GENRE = False
    TABLES = True
    CONNECT = False
    YOUTUBE = False
    TOP = False
    SAVED = False
    PLAYLIST = False
    FOLLOWS = False

    test_token = os.getenv('USER_TEST_TOKEN')
    
    if USER:
        start_time = time.time()
        result = get_user_info(test_token)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')
        
        print(result)

    if GENRE:
        headers = get_auth_header(test_token)
        result = {
            'users': get_user_info(test_token),
            'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
            'artists': pd.DataFrame(columns=['artist_id', 'name']),
            'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
            'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
            'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
            'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
        }
        user_id = result['users']['user_id'].iloc[0]

        result = get_top(test_token, headers, user_id, result)
        for key in result:
            result[key] = result[key].drop_duplicates()
        start_time = time.time()
        result = get_genres(test_token, headers, user_id, result)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')

        print(f'Artist Genres:\n{len(result['artist_genres'])} rows\n{result['artist_genres']['artist_id'].nunique()} unique artist ID\'s\n{result['artist_genres'].head(3)}')

    if TABLES:
        start_time = time.time()
        result = get_all_tables(test_token)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')

        for key in result:
            print(f'{key}: {len(result[key])} rows')
            print(result[key].head(3))

    if CONNECT:
        print('Connecting to database...')
        cursor = connect_to_db()
        if cursor:
            print('Connected to database successfully')

    if YOUTUBE:
        start_time = time.time()
        link = get_youtube_preview('Take Me Out', 'Franz Ferdinand')
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')

        print(link)

    if TOP:
        headers = get_auth_header(test_token)
        result = {
            'users': get_user_info(test_token),
            'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
            'artists': pd.DataFrame(columns=['artist_id', 'name']),
            'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
            'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
            'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
            'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
        }
        user_id = result['users']['user_id'].iloc[0]
        start_time = time.time()
        result = get_top(test_token, headers, user_id, result)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')

        for key in result:
            df = result[key]
            print(f'{key}: {len(df)} rows\n{df.head(3)}\n')

    if SAVED:
        headers = get_auth_header(test_token)
        result = {
            'users': get_user_info(test_token),
            'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
            'artists': pd.DataFrame(columns=['artist_id', 'name']),
            'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
            'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
            'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
            'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
        }
        user_id = result['users']['user_id'].iloc[0]
        start_time = time.time()
        result = get_all_saved_tracks(test_token, headers, user_id, result)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')

        for key in result:
            df = result[key]
            print(f'{key}: {len(df)} rows\n{df.head(3)}\n')

    if PLAYLIST:
        headers = get_auth_header(test_token)
        result = {
            'users': get_user_info(test_token),
            'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
            'artists': pd.DataFrame(columns=['artist_id', 'name']),
            'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
            'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
            'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
            'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
        }
        user_id = result['users']['user_id'].iloc[0]
        start_time = time.time()
        result = get_all_playlist_tracks(test_token, headers, user_id, result)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')
        
        for key in result:
            df = result[key]
            print(f'{key}: {len(df)} rows\n{df.head(3)}\n')

    if FOLLOWS:
        headers = get_auth_header(test_token)
        result = {
            'users': get_user_info(test_token),
            'songs': pd.DataFrame(columns=['song_id', 'title', 'img_url', 'preview_url']),
            'artists': pd.DataFrame(columns=['artist_id', 'name']),
            'artist_genres': pd.DataFrame(columns=['artist_id', 'genre']),
            'user_song_interactions': pd.DataFrame(columns=['user_id', 'song_id', 'saved', 'top_song', 'playlist']),
            'user_artist_interactions': pd.DataFrame(columns=['user_id', 'artist_id', 'follows', 'top_artist']),
            'song_artist_interactions': pd.DataFrame(columns=['song_id', 'artist_id'])
        }
        user_id = result['users']['user_id'].iloc[0]
        start_time = time.time()
        result = get_followed_artists(test_token, headers, user_id, result)
        end_time = time.time()
        print(f'Completed in {end_time - start_time} seconds')
        
        for key in result:
            df = result[key]
            print(f'{key}: {len(df)} rows\n{df.head(3)}\n')
