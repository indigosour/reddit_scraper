import subprocess
import praw
import streamlit as st # app dev 
import requests # download json, mp4 
import json # json parsing
import uuid
import os

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

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


def get_mp4_urllist(sub,period):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    mp4_urllist = []
    for reddit_url in get_reddit_list(f'{sub}',f'{period}'):
        if reddit_url["permalink"]:
            id = reddit_url["id"]
            if reddit_url["permalink"][len(reddit_url["permalink"])-1] == '/':
                json_url = reddit_url["permalink"][:len(reddit_url["permalink"])-1]+'.json'
            else:
                json_url = reddit_url["permalink"] + '.json'

            json_response = requests.get(json_url, 
                            headers= headers)

            if json_response.json()[0]['data']['children'][0]['data']['secure_media'] == None:
                break
            elif json_response.status_code != 200:
                print("Error Detected, check the URL!!!")
            else:
                mp4_url = json_response.json()[0]['data']['children'][0]['data']['secure_media']['reddit_video']['fallback_url']    
            json_urllist = {
                    "url": mp4_url,
                    "id": id
                    }
            mp4_urllist.append(json_urllist)
    return mp4_urllist

# Download MP4 and combine video/audio with ffmpeg

def download_video(name,url,path):
    # Setup Audio URL
    front = ((url.index('_')) +1)
    fslice = url[0:front]
    audio_url = fslice + 'audio.mp4'

    # Download video
    fullpath=path + '/' + name + "_video.mp4"
    r=requests.get(f'{url}')
    f=open(fullpath,'wb');
    print("Donloading video.....")
    for chunk in r.iter_content(chunk_size=255): 
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
    print("Done")
    f.close()
    video_fullpath = fullpath
    #print(video_fullpath)

    # Download audio
    fullpath=path + '/' + name + "_audio.mp3"
    r=requests.get(f'{audio_url}')
    f=open(fullpath,'wb');
    print("Donloading audio.....")
    for chunk in r.iter_content(chunk_size=255): 
        if chunk: # filter out keep-alive new chunks
            f.write(chunk)
    print("Done")
    f.close()
    audio_fullpath = fullpath
    #print(audio_fullpath)

    # Combine video and audio files
    subprocess.call(['/usr/bin/ffmpeg','-i',f'{video_fullpath}','-i',f'{audio_fullpath}','-map','0:v','-map','1:a','-c:v','copy',f'{name}_combined.mp4'])
    subprocess.call(['rm',f'{video_fullpath}',f'{audio_fullpath}'])
    subprocess.call(['mv',f'{name}_combined.mp4', f'{path}/{name}_combined.mp4'])

# Input subreddit and period of time to create working directory and collect mp4 files
def main(sub,period):
    playlist = get_mp4_urllist(f'{sub}',f'{period}')
    uuid_value = str(uuid.uuid4())
    parent_dir = "/home/john/code_repo/reddit_scraper/storage/"
    path = os.path.join(parent_dir, uuid_value)
    os.mkdir(path)

    for url in playlist:
        download_video(url["id"],url["url"],path)