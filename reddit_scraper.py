import praw
import uuid
import os
from redvid import Downloader

reddit_read_only = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
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

# Download MP4 and combine video/audio with ffmpeg

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()

# Input subreddit and period of time to create working directory and collect mp4 files
def main(sub,period):
    playlist = get_reddit_list(f'{sub}',f'{period}')
    uuid_value = str(uuid.uuid4())
    parent_dir = "/home/john/code_repo/reddit_scraper/storage/"
    path = os.path.join(parent_dir, uuid_value)
    os.mkdir(path)

    for post in playlist:
        download_video(post["permalink"],path)