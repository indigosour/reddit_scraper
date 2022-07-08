import praw
import uuid
import os
import glob
from redvid import Downloader
import ffmpeg
import requests
from datetime import date

#Peertube
from __future__ import print_function
import time
import peertube
from peertube.rest import ApiException
from pprint import pprint

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


def upload_video(video_path):
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoApi(api_client)

    videofile = video_path # file | Video file
    channel_id = 56 # int | Channel id that will contain this video
    name = 'name_example' # str | Video name
    privacy = peertube.VideoPrivacySet() # VideoPrivacySet |  (optional)
    category = 56 # int | Video category (optional)
    description = 'description_example' # str | Video description (optional)
    wait_transcoding = True # bool | Whether or not we wait transcoding before publish the video (optional)
    tags = 'tags_example' # list[str] | Video tags (maximum 5 tags each between 2 and 30 characters) (optional)

    try:
        # Upload a video
        api_response = api_instance.videos_upload_post(videofile, channel_id, name, privacy=privacy, category=category, description=description, wait_transcoding=wait_transcoding, tags=tags)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)


# Input subreddit and period of time to create working directory and collect mp4 files
def main(sub,period):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    playlist = get_video_posts(f'{sub}',f'{period}')
    uuid_value = str(uuid.uuid4())
    parent_dir = working_dir
    path = os.path.join(parent_dir, uuid_value)
    os.mkdir(path)

    for post in playlist:
        print (post["permalink"])
        download_video(post["permalink"],path)
        active_path = glob.glob(f"{path}/*.mp4")
        dest_path = f'{path}/{post["title"]}.mp4'
        os.rename(active_path, dest_path)

    print("Files downloaded successfully")

    with open("output.txt", "w") as a:
        for path, subdirs, files in os.walk(f'{path}'):
            for filename in files:
                f = os.path.join(path, filename)
                a.write(f'file {f}' + os.linesep) 

    print("Merging files now...")

    # Merging all videos together
    #ffmpeg.input('output.txt', f='concat', safe=0).output(f'{sub}_{period}_{date.isoformat(today)}.mp4').run()

    print("Done! Enjoy your content!")