import praw
import uuid
import os
import glob
from redvid import Downloader
import requests
import datetime
import time
import pprint

#Peertube
from __future__ import print_function
import peertube
from peertube.rest import ApiException

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

configuration = peertube.Configuration(
    host = "***REMOVED***"
)
configuration.access_token = 'd3a24a4116a73a8b123cdbc9e232de21a290ded5'


# Variables
working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/storage/"

# Get reddit list for <sub> - videos,tiktokcringe,etc and <period> - day, week, month, year
# Output: json object containing id, title, author, score, permalink, and flare

def get_reddit_list(sub,period):
    subreddit = reddit_read_only.subreddit(f'{sub}')
    posts = subreddit.top(f'{period}')
    postlist = []
    for post in posts:
        postlist.append({
        "id": post.id,
        "title": post.title,
        "author": post.author,
        "score": post.score,
        "permalink": "https://reddit.com" + post.permalink,
        "flare": post.link_flair_text,
        })
    return postlist

# Check for videos and collect list

def get_video_posts(sub,period):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    video_urllist = []
    #sub = 'funny'
    #period = 'day'
    for reddit_url in get_reddit_list(f'{sub}',f'{period}'):
        if reddit_url["permalink"]:
            if reddit_url["permalink"][len(reddit_url["permalink"])-1] == '/':
                json_url = reddit_url["permalink"][:len(reddit_url["permalink"])-1]+'.json'
            else:
                json_url = reddit_url["permalink"] + '.json'
            json_response = requests.get(json_url, 
                            headers= headers)
            if json_response.json()[0]['data']['children'][0]['data']['is_video'] == True and json_response.json()[0]['data']['children'][0]['data']['over_18'] == False:
                json_urllist = {
                    "id": json_response.json()[0]['data']['children'][0]['data']['id'],
                    "title": json_response.json()[0]['data']['children'][0]['data']['title'],
                    "permalink": reddit_url["permalink"],
                    }
                video_urllist.append(json_urllist)
            elif json_response.status_code != 200:
                print("Error Detected, check the URL!!!")
    return video_urllist


# Download MP4 - v.reddit.it

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()


# Upload video to peertube instance

def upload_video(video_path,title,sub):
    # video_path = "/home/john/code_repo/reddit_scraper/unexpected_day_2022-07-05.mp4"
    # title = 'unexpected_day_2022-07-05'
    # sub = "unexpected"
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoApi(api_client)

    if sub == 'unexpected':
        channel_id = 6
    elif sub == 'publicfreakout':
        channel_id = 7
    else:
        print('Not a valid channel_id')

    videofile = video_path # file | Video file
    name = title # str | Video name
    privacy = 1 # VideoPrivacySet |  (optional)
    # category = 56 # int | Video category (optional)
    # description = 'description_example' # str | Video description (optional)
    # wait_transcoding = True # bool | Whether or not we wait transcoding before publish the video (optional)
    # tags = 'tags_example' # list[str] | Video tags (maximum 5 tags each between 2 and 30 characters) (optional)

    try:
        # Upload a video
        api_response = api_instance.videos_upload_post(videofile, channel_id, name, privacy=privacy)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)
    # return api_response

# Create playlist in peertube

def create_playlist(display_name,sub):
    # Enter a context with an instance of the API client
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoPlaylistsApi(api_client)
    privacy = 1 # VideoPlaylistPrivacySet |  (optional)
    #description = 'description_example' # str | Video playlist description (optional)
    video_channel_id = sub # int | Video channel in which the playlist will be published (optional)

    try:
        # Create a video playlist
        api_response = api_instance.video_playlists_post(display_name, privacy=privacy, video_channel_id=video_channel_id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VideoPlaylistsApi->video_playlists_post: %s\n" % e)


# Add video to playlist

def add_video_playlist(v_id):
    # Enter a context with an instance of the API client
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoPlaylistsApi(api_client)
        id = v_id

    try:
        # Add a video in a playlist
        api_response = api_instance.video_playlists_id_videos_post(id)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VideoPlaylistsApi->video_playlists_id_videos_post: %s\n" % e)
    

# Input subreddit and period of time to create working directory and collect mp4 files

def main(sub,period):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    date = str(datetime.date.today())
    playlist = get_video_posts(f'{sub}',f'{period}')
    uuid_value = str(uuid.uuid4())
    parent_dir = working_dir
    path = os.path.join(parent_dir, uuid_value)
    os.mkdir(path)

    if sub == "unexpected":
        channel_id = 6
        proper = "Unexpected"
    elif sub == "publicfreakout":
        channel_id = 7
        proper = "Public Freakout"
    else:
        print('Not a valid channel_id')

    create_playlist(f'{proper} - {period} - {date}',sub)
    

    for post in playlist:
        # Download video from Redvid
        download_video(post["permalink"],path)
        print("Video download complete")
        
        active_path = glob.glob(f"{path}/*.mp4")
        
        # Upload video to peertube
        upload_video(active_path[0],post["title"],sub)
        print("Video upload complete")
        
        # Add video to playlist
        add_video_playlist(channel_id)
        print("Video added to playlist")

        time.sleep(1)
        os.remove(active_path[0])
        print("Deleting video file")

    print("Files downloaded successfully")

    # with open("output.txt", "w") as a:
    #     for path, subdirs, files in os.walk(f'{path}'):
    #         for filename in files:
    #             f = os.path.join(path, filename)
    #             a.write(f'file {f}' + os.linesep) 

    # Merging all videos together
    #ffmpeg.input('output.txt', f='concat', safe=0).output(f'{sub}_{period}_{date.isoformat(today)}.mp4').run()