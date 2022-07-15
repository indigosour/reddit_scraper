import praw, os, glob, requests
from datetime import datetime
from pathlib import Path
from redvid import Downloader

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

# Variables
working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"
storage_dir = (os.path.dirname(os.path.realpath(__file__))) + "/storage"

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
                    "permalink": reddit_url["permalink"],
                    "title": json_response.json()[0]['data']['children'][0]['data']['title']
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

# Input subreddit and period of time to create working directory and collect mp4 files
def main(sub,period):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    today = datetime.today().strftime('%m-%d-%Y')
    playlist = get_video_posts(f'{sub}',f'{period}')
    # uuid_value = str(uuid.uuid4())
    # parent_dir = working_dir
    # path = f'{storage_dir}/{sub}_{period}_{today}/'
    # os.mkdir(path)

    for post in playlist:
        print (post["permalink"])
        download_video(post["permalink"],working_dir)

        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_{period}_{today}/{post["title"]}.mp4'
        Path(old_filename[0]).rename(new_filename)
        

    filenames = glob.glob(f'{storage_dir}/{sub}_{period}_{today}/{post["title"]}*.mp4')
    return filenames

# subprocess.run(["cat", "*.mp4","|","ffmpeg","-i","pipe:","-c:a","copy","-c:v","copy all.mp4"])
# cat *.mp4  | ffmpeg  -i pipe: -c:a copy -c:v copy all.mp4