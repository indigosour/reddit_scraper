from pip import main
import praw, os, glob, requests, time
from datetime import datetime
from pathlib import Path
from redvid import Downloader

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

# Variables
working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"
storage_dir = (os.path.dirname(os.path.realpath(__file__))) + "/storage"

def cleanFilename(sourcestring,  removestring ="%:/,.\\[]<>*?"):
    """Clean a string by removing selected characters.

    Creates a legal and 'clean' source string from a string by removing some 
    clutter and  characters not allowed in filenames.
    A default set is given but the user can override the default string.

    Args:
        | sourcestring (string): the string to be cleaned.
        | removestring (string): remove all these characters from the string (optional).

    Returns:
        | (string): A cleaned-up string.

    Raises:
        | No exception is raised.
    """
    #remove the undesireable characters
    return ''.join([c for c in sourcestring if c not in removestring])


# Get reddit list for <sub> - videos,tiktokcringe,etc and <period> - day, week, month, year
# Output: json object containing id, title, author, score, permalink, and flare

def get_reddit_list_period(sub,period):
    subreddit = reddit_read_only.subreddit(f'{sub}')
    posts = subreddit.top(f'{period}',limit=200)
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


# Get reddit list for <sub> - videos,tiktokcringe,etc and <num> - number of top posts to pull
# # Output: json object containing id, title, author, score, permalink, and flare

def get_reddit_list_number(sub,num):
    posts = reddit_read_only.subreddit(f'{sub}').top(limit=num)
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

def get_video_posts_period(sub,period):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    video_urllist = []
    #sub = 'funny'
    #period = 'day'
    for reddit_url in get_reddit_list_period(f'{sub}',f'{period}'):
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


# Check for videos and collect list

def get_video_posts_num(sub,num):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    video_urllist = []
    #sub = 'funny'
    #num = 500
    for reddit_url in get_reddit_list_number(f'{sub}',num):
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


# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_period(sub,period):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    today = datetime.today().strftime('%m-%d-%Y')
    playlist = get_video_posts_period(f'{sub}',f'{period}')
    # uuid_value = str(uuid.uuid4())
    # parent_dir = working_dir
    path = f'{storage_dir}/{sub}_{period}_{today}/'
    os.mkdir(path)

    for post in playlist:
        print (post["permalink"])
        sani_title = cleanFilename(post["title"])
        download_video(post["permalink"],working_dir)
        time.sleep(0.500)
        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_{period}_{today}/{sani_title}.mp4'
        try:
            Path(old_filename[0]).rename(new_filename)
        except:
            continue
        print(f'Moving {old_filename[0]} to {new_filename}')
        

# Download by sub and number = Input subreddit and number of top posts to collect to create working directory and collect mp4 files

def main_num(sub,num):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    today = datetime.today().strftime('%m-%d-%Y')
    playlist = get_video_posts_num(f'{sub}',num)
    # uuid_value = str(uuid.uuid4())
    # parent_dir = working_dir
    path = f'{storage_dir}/{sub}_Top_{num}_{today}/'
    os.mkdir(path)

    for post in playlist:
        print (post["permalink"])
        sani_title = cleanFilename(post["title"])
        download_video(post["permalink"],working_dir)
        time.sleep(0.500)
        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_Top_{num}_{today}/{sani_title}.mp4'
        try:
            Path(old_filename[0]).rename(new_filename)
        except:
            continue
        print(f'Moving {old_filename[0]} to {new_filename}')


# subprocess.run(["cat", "*.mp4","|","ffmpeg","-i","pipe:","-c:a","copy","-c:v","copy all.mp4"])
# cat *.mp4  | ffmpeg  -i pipe: -c:a copy -c:v copy all.mp4

def content(period):
    sublist = [
        
        "tiktokcringe",     # https://reddit.com/r/tiktokcringe
        "unexpected",       # https://reddit.com/r/unexpected
        "funny",            # https://reddit.com/r/funny
        "whatcouldgowrong", # https://reddit.com/r/whatcouldgowrong
        "eyebleach",        # https://reddit.com/r/eyebleach
        "humansbeingbros"   # https://reddit.com/r/humansbeingbros
    ]
    for sub in sublist:
        main_period(f'{sub}',f'{period}')
        print(f'Finished downloading {sub}...')