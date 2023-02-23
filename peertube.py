import logging,requests,json,os

# Variables
peertube_api_url = "***REMOVED***"

######################
##### Peertube #######
######################

def peertube_auth():
    global peertube_token
    peertube_api_user = "***REMOVED***"
    peertube_api_pass = "***REMOVED***"
    logging.info("peertube_auth: Logging into peertube")

    try:
        response = requests.get(peertube_api_url + '/oauth-clients/local')
        data = response.json()
        client_id = data['client_id']
        client_secret = data['client_secret']

        data = {
            'client_id': client_id,
            'client_secret': client_secret,
            'grant_type': 'password',
            'response_type': 'code',
            'username': peertube_api_user,
            'password': peertube_api_pass
        }

        response = requests.post( peertube_api_url + '/users/token', data=data)
        data = response.json()
        peertube_token = data['access_token']
    except Exception as e:
        logging.error('peertube_auth: Error logging into peertube.',e)


def list_channels():
    global peertube_token
    headers = {
	'Authorization': 'Bearer' + ' ' + peertube_token
    }
    params={'count': 50,'sort': '-createdAt'}
    channel_list = {}
    res = requests.get(url=f'{peertube_api_url}/video-channels', headers=headers, params=params)

    try:
        for i in res.json()['data']:
            channel_list[i['displayName'].replace("r/","")] = i['id']
    except json.decoder.JSONDecodeError as e:
        print(f"Error decoding JSON: {e}")
        print(f"Response content: {res.content}")
        logging.error(f"list_channels: Error decoding JSON: {res.content} {e}")
    
    return channel_list


# Upload video to peertube instance

# def upload_video(sub,title,video_path):
#     videoChannelId = list_channels()[sub]
#     filenamevar = os.path.basename(video_path)
#     data = {'channelId': videoChannelId, 'name': title, 'privacy': 1}
#     files = {
#         'videofile': (filenamevar,open(video_path, 'rb'),'video/mp4',{'Expires': '0'})}
#     headers = {
#             'Authorization': 'Bearer ' + peertube_auth()
#         }
#     try:
#         # Upload a video
#             res = requests.post(url=f'{peertube_api_url}/videos/upload',headers=headers,files=files,data=data)
#             print(f'Failed to upload {res.text}')
#             v_id = res.json()['video']['uuid']

#     except Exception as e:
#         print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)
#     return v_id


def upload_video(sub,title,video_path):
    global peertube_token
    try:
        videoChannelId = list_channels()[sub]
        filenamevar = os.path.basename(video_path)
        data = {'channelId': videoChannelId, 'name': title, 'privacy': 1}
        files = {
            'videofile': (filenamevar,open(video_path, 'rb'),'video/mp4',{'Expires': '0'})}
        headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
        
        # Upload a video
        res = requests.post(url=f'{peertube_api_url}/videos/upload', headers=headers, files=files, data=data)
        res.raise_for_status()
        v_id = res.json()['video']['uuid']
        print(f"Successfully uploaded video with id {v_id}")
        return v_id
    
    except Exception as e:
        print(f"Error occurred while uploading video: {e}")
        logging.error(f"upload_video: Error occurred while uploading video: {e}")
        return None


# Create playlist in peertube

def create_playlist(display_name,sub):
    global peertube_token
    videoChannelId = list_channels()[sub]
    logging.info(f'create_playlist: Creating playlist {display_name} from {sub}')
    privacy = 1
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    data = {
        'displayName': (None, display_name),
        'videoChannelId': (None, videoChannelId),
        'privacy': (None, str(privacy))
    }

    try:
        # Create playlilst
            res = requests.post(url=f'{peertube_api_url}/video-playlists',headers=headers,files=data)
            p_id = res.json()['videoPlaylist']['id']
    except Exception as e:
        print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)
        logging.error(f"create_playlist: Exception when calling VideoApi->videos_upload_post: {e}")
    return p_id


# # Add video to playlist

def add_video_playlist(v_id,p_id):
    global peertube_token
    headers = {
            'Authorization': 'Bearer ' + peertube_token
        }
    data = {
        'videoId': v_id
    }
    try:
        # Create playlilst
            res = requests.post(url=f'{peertube_api_url}/video-playlists/{p_id}/videos',headers=headers,json=data)
    except Exception as e:
        print(f'Error adding video to playlist {p_id}')
        logging.error(f'add_video_playlist: Error adding video to playlist {p_id}')
    return logging.info(f'Video {v_id} added successfully to playlist {p_id}.')
