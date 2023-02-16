import mysql.connector as db
import os,time,praw,glob,requests
from videohash import VideoHash
from pathlib import Path
from redvid import Downloader
from datetime import datetime, timedelta
import peertube
from peertube.rest import ApiException
from pprint import pprint

debug = False

reddit = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

# Variables and config

peertube_api_url = "https://tubelrone.com/api/v1"
peertube_api_user = "autoupload1"
peertube_api_pass = "BfWnTPT#Z@vF%Y6BPV%8xMjp%7vKS5"

response = requests.get(peertube_api_url + '/oauth-clients/local')
data = response.json()
client_id = data['client_id']
client_secret = data['client_secret']



working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"
storage_dir = "/mnt/appstorage/video"

AZURE_STORAGE_ACCOUNT_NAME = "tubelrone"
AZURE_STORAGE_CONTAINER_NAME = "appstorage"

sublist = [

            "unexpected",
            "funny",
            "whatcouldgowrong",
            "eyebleach",
            "humansbeingbros",
            "contagiouslaughter",
            "unexpectedthuglife",
            "therewasanattempt",
            "damnthatsinteresting",
            "nextfuckinglevel",
            "oddlysatisfying",
            "animalsbeingbros",
            "funnyanimals",
            "maybemaybemaybe",
            "beamazed",
            "aww",
            "tiktokcringe",
            "blackmagicfuckery",
            "mademesmile",
            "dankvideos",
            "perfectlycutscreams",
            "praisethecameraman",
            "publicfreakout",
            "peoplefuckingdying",
            "yesyesyesyesno",
            "animalsbeingjerks",
            "nonononoyes"

        ]

def cleanString(sourcestring,  removestring ="%:/,.\"\\[]<>*?"):
    #remove the undesireable characters
    return ''.join([c for c in sourcestring if c not in removestring])


######################
######## SQL #########
######################

def create_db_connection():
    connection = None
    user_name = "python_u"
    user_password = "GxVpw3zwBYx7eJ8uX2jW844du4Bc2m"
    host_name = "172.18.0.3"
    db_name = "reddit_scraper"
    try:
        connection = db.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
    except db.Error as err:
        print(f"Error: '{err}'")
    return connection


def create_sub_table(sub):
    connection = create_db_connection()
    cursor = connection.cursor()
    try:
        statement = f"""
        
        CREATE TABLE subreddit_{sub} (
            id varchar(255),
            title varchar(255),
            author varchar(255),
            score int,
            upvote_ratio DECIMAL,
            num_comments int,
            created_utc DATETIME,
            flair varchar(255),
            is_original_content BOOL,
            is_self BOOL,
            over_18 BOOL,
            stickied BOOL,
            permalink varchar(255),
            path varchar(255),
            videohash binary(66),
            is_downloaded BOOL,
            PRIMARY KEY (id)
                );
        
        """
        cursor.execute(statement)
        connection.commit()
        print(f'Successfully created table subreddit_{sub} in the db')
        cursor.close()
    except db.Error as e:
        print("Error creating table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()


def drop_table(table):
    connection = create_db_connection()
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        connection.commit()
        print(f'{table} dropped')
    except db.Error as e:
        print("Error dropping table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()


def store_reddit_posts(sub, postlist):
    connection = create_db_connection()
    cursor = connection.cursor()
    entrycount = 0
    for post in postlist:
        id = post['id']
        title = post['title']
        author = post['author']
        score = post['score']
        upvote_ratio = post['upvote_ratio']
        num_comments = post['num_comments']
        created_utc = post['created_utc']
        flair = post['flair']
        is_original_content = post['is_original_content']
        is_self = post['is_self']
        over_18 = post['over_18']
        stickied = post['stickied']
        permalink = post['permalink']

        try:
            statement = f"""
            
            INSERT INTO subreddit_{sub} (id,title,author,score,upvote_ratio,num_comments,
            created_utc,flair,is_original_content,is_self,over_18,stickied,permalink,path,videohash,is_downloaded) 
            VALUES ("{id}","{title}","{author}",{score},{upvote_ratio},{num_comments},"{created_utc}",
            "{flair}",{is_original_content},{is_self},{over_18},{stickied},"{permalink}",NULL,NULL,NULL)
            
            """
            #print(statement)
            cursor.execute(statement)
            connection.commit()
            #print("Successfully added entry to database")
            entrycount+=1
        except db.Error as e:
            if e.errno != 1062:
                print("Error inserting data into table", e)
            continue
    if connection.is_connected():
        connection.close()
        cursor.close()
    return print(f"Successfully added {entrycount} entries to database")


def get_dl_list_period(sub,period):
    connection = create_db_connection()
    cursor = connection.cursor()

    # Determine the start and end dates based on the period given
    if period == "day":
        start_date = str(datetime.today() - timedelta(days=1))
        end_date = str(datetime.today())
    elif period == "week":
        start_date = str(datetime.today() - timedelta(days=7))
        end_date = str(datetime.today())
    elif period == "month":
        start_date = str(datetime.today() - timedelta(days=30))
        end_date = str(datetime.today())
    elif period == "year":
        start_date = str(datetime.today() - timedelta(days=365))
        end_date = str(datetime.today())
    else:
        print("Invalid period")

    print (f"Period calculated \nStart Date: {start_date} and End Date: {end_date}")

    try:
        select_Query = f"""
        
        SELECT title,permalink,id from subreddit_{sub}
        WHERE created_utc BETWEEN "{start_date}" AND "{end_date}"
        
        """
        cursor.execute(select_Query)
        dl_list = cursor.fetchall()

        # for post in dl_list:
        #     print(post[0])
        #     print(post[1])

    except db.Error as e:
        print("Error reading data from table", e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
    return dl_list


def update_item_db(sub,v_hash,id,blob_url):
    connection = create_db_connection()
    cursor = connection.cursor()
    try:
        sql = "UPDATE subreddit_{} SET videohash = '{}', path = '{}' WHERE id = '{}'".format(sub,v_hash,blob_url,id)
        #print(sql)

        cursor.execute(sql)

        connection.commit()

        print(cursor.rowcount, "record(s) updated")

    except db.Error as e:
        print("Error inserting data into table", e)

####### END SQL FUNCTIONS #######


######################
####### REDDIT #######
######################

# get_reddit_list: Get reddit posts and store in a list

# Get the requested number of new posts from the subreddit with minimum upvotes required
# and return a post list to upload to the database.

# Submit: Subreddit (sub) and number (num) of posts to get
# Return: Number of posts requested including id, title, author, score, etc.

def get_reddit_list(sub,period):
    num = 1000
    posts = reddit.subreddit(f'{sub}').top(time_filter=f'{period}',limit=num)
    
    postlist = []
    
    for post in posts:
        if post.author != None and post.is_video == True:
            try:
                if debug: 
                    print(post)
                postlist.append({
                "id": post.id,
                "title": cleanString(post.title),
                "author": post.author.name,
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_utc": str(datetime.fromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S')).strip(),
                "flair": post.link_flair_text,
                "is_original_content": post.is_original_content,
                "is_self": post.is_self,
                "over_18": post.over_18,
                "stickied": post.stickied,
                "permalink": "https://reddit.com" + post.permalink,
                })            
            except Exception as e:
                print(e)
    return postlist

# Download video posts

def download_video(url,path):
    try:
        reddit = Downloader(max_q=True)
        reddit.url = url
        reddit.path = path
        reddit.download()
    except Exception as e:
        print(f"Could not download video. Error: {str(e)}")


######################
##### Peertube #######
######################

def get_token():
    global client_id, client_secret, peertube_api_user, peertube_api_pass
    data = {
        'client_id': client_id,
        'client_secret': client_secret,
        'grant_type': 'password',
        'response_type': 'code',
        'username': peertube_api_user,
        'password': peertube_api_pass
    }

    response = requests.post(peertube_api_url + '/users/token', data=data)
    data = response.json()
    token_type = data['token_type']
    access_token = data['access_token']

    return access_token

configuration = peertube.Configuration(
host = f'{peertube_api_url}'
)
configuration.access_token = get_token()


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
        api_out = str(api_response).split("uuid")[1].replace(":","").replace('\'',"").replace("}}","").replace(" ", "")
    except ApiException as e:
        print("Exception when calling VideoApi->videos_upload_post: %s\n" % e)
    return api_out


# Create playlist in peertube

def create_playlist(display_name,channel_id):
    # Enter a context with an instance of the API client
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoPlaylistsApi(api_client)
    privacy = 1 # VideoPlaylistPrivacySet |  (optional)
    #description = 'description_example' # str | Video playlist description (optional)
    video_channel_id = channel_id # int | Video channel in which the playlist will be published (optional)
    try:
        # Create a video playlist
        api_response = api_instance.video_playlists_post(display_name, privacy=privacy, video_channel_id=video_channel_id)
        api_out = str(api_response).split("uuid")[1].replace(":","").replace('\'',"").replace("}}","").replace(" ", "")
    except ApiException as e:
        print("Exception when calling VideoPlaylistsApi->video_playlists_post: %s\n" % e)
    return api_out


# Add video to playlist

def add_video_playlist(v_id,p_id):
    configuration = peertube.Configuration(host = f'{peertube_api_url}/video-playlists/{p_id}/videos')
    #configuration.access_token = get_token()
    # Enter a context with an instance of the API client
    with peertube.ApiClient(configuration) as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoPlaylistsApi(api_client)
    try:
        # Add a video in a playlist
        api_instance.video_playlists_id_videos_post(v_id)
    except ApiException as e:
        print("Exception when calling VideoPlaylistsApi->video_playlists_id_videos_post: %s\n" % e)


# Create channels

def create_channel(sub):
    with peertube.ApiClient(configuration) as api_client:
    # Create an instance of the API class
        api_instance = peertube.VideoChannelsApi(api_client)
        video_channel_create = peertube.VideoChannelCreate(name=f'{sub}',display_name=f'r/{sub}') # VideoChannelCreate |  (optional)

    try:
        # Create a video channel
        api_instance.video_channels_post(video_channel_create=video_channel_create)
    except ApiException as e:
        print("Exception when calling VideoChannelsApi->video_channels_post: %s\n" % e)


# Get Video Channels

def get_channels():
    with peertube.ApiClient() as api_client:
        # Create an instance of the API class
        api_instance = peertube.VideoChannelsApi(api_client)
        name = 'autoupload1' # str | The name of the account
    with_stats = True # bool | include view statistics for the last 30 days (only if authentified as the account user) (optional)
    start = 56 # int | Offset used to paginate results (optional)
    count = 15 # int | Number of items to return (optional) (default to 15)
    sort = '-createdAt' # str | Sort column (optional)

    try:
        # List video channels of an account
        api_response = api_instance.accounts_name_video_channels_get(name, with_stats=with_stats, start=start, count=count, sort=sort)
        pprint(api_response)
    except ApiException as e:
        print("Exception when calling VideoChannelsApi->accounts_name_video_channels_get: %s\n" % e)



###############################
##### MAIN FUNCTIONS ##########
###############################

# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_dl_period(sub,period):
    #sub = 'funny'
    #period = 'day'
    global working_dir
    today = datetime.today().strftime('%m-%d-%Y')
    dlList = get_dl_list_period(f'{sub}',f'{period}')
    # uuid_value = str(uuid.uuid4())
    # parent_dir = working_dir
    path = f'{storage_dir}/{sub}_{period}_{today}/'
    isExist = os.path.exists(path)
    if not isExist:
        os.mkdir(path)
    
    # Download each video post
    for post in dlList:
        print (post[1])
        sani_title = cleanString(post[0])
        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_{period}_{today}/{sani_title}.mp4'
        id = post[2]
        url = post[1]

        # Download video and store in working directory
        try:
            download_video(url,working_dir)
        except:
            return
        time.sleep(0.500)

        # Rename video file
        try:
            Path(old_filename[0]).rename(new_filename)
        except:
            return

        v_hash = VideoHash(path=f"{new_filename}")
        url = str(post[1])
        hash_value = v_hash.hash
        if debug:
            print(f'Video hash: {hash_value}')

        folder = f'{sub}_{period}_{today}'

        # Add video hash and path to database
        update_item_db(sub,hash_value,id,path)

        if debug:
            print(path)
        print(f'Moving {old_filename[0]} to {new_filename}')


def update_DB():
    global sublist
    for sub in sublist:
        # drop_table(f"subreddit_{sub}")
        # create_sub_table(f"{sub}")
        print(f"Table {sub} created moving on to download posts")
        for period in ["week","month","year","all"]:
            print(f'Gathering top posts from {sub} for the {period}...')
            postlist = get_reddit_list(sub,period)
            print(f'Found {len(postlist)} posts. Now storing them...')
            store_reddit_posts(sub,postlist)
            print(f'Finished storing top of {sub} for the {period}')


def grab_dat(period):
    global sublist
    for sub in sublist:
        main_dl_period(sub, period)


#### OLD ####

# Download by sub and number = Input subreddit and number of top posts to collect to create working directory and collect mp4 files

# def main_num(sub,num):
#     #sub = 'funny'
#     #period = 'day'
#     global working_dir
#     today = datetime.today().strftime('%m-%d-%Y')
#     playlist = get_video_posts_num(f'{sub}',num)
#     # uuid_value = str(uuid.uuid4())
#     # parent_dir = working_dir
#     path = f'{storage_dir}/{sub}_Top_{num}_{today}/'
#     os.mkdir(path)

#     for post in playlist:
#         print (post["permalink"])
#         sani_title = cleanString(post["title"])
#         download_video(post["permalink"],working_dir)
#         time.sleep(0.500)
#         old_filename = glob.glob(f"{working_dir}/*.mp4")
#         new_filename = f'{storage_dir}/{sub}_Top_{num}_{today}/{sani_title}.mp4'
#         try:
#             Path(old_filename[0]).rename(new_filename)
#         except:
#             continue
#         print(f'Moving {old_filename[0]} to {new_filename}')

# def get_video_posts_num(sub,num):
#     headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#     video_urllist = []
#     #sub = 'funny'
#     #num = 500
#     for post in get_reddit_list_number(f'{sub}',num):
#         if post["permalink"]:
#             if post["permalink"][len(post["permalink"])-1] == '/':
#                 json_url = post["permalink"][:len(post["permalink"])-1]+'.json'
#             else:
#                 json_url = post["permalink"] + '.json'
#             json_response = requests.get(json_url, 
#                             headers= headers)
#             if json_response.json()[0]['data']['children'][0]['data']['is_video'] == True and json_response.json()[0]['data']['children'][0]['data']['over_18'] == False:
#                 json_urllist = {
#                     "id": json_response.json()[0]['data']['children'][0]['data']['id'],
#                     "permalink": post["permalink"],
#                     "title": json_response.json()[0]['data']['children'][0]['data']['title']
#                     }
#                 video_urllist.append(json_urllist)
#             elif json_response.status_code != 200:
#                 print("Error Detected, check the URL!!!")
#     return video_urllist  

#### Pushshift.io Method #######

# def get_reddit_list(sub):

#     if sub == "tiktokcringe":
#         required_score = 1000
#     elif sub == "unexpected":
#         required_score = 700
#     elif sub == "funny":
#         required_score = 500
#     elif sub == "whatcouldgowrong":
#         required_score = 500
#     elif sub == "eyebleach":
#         required_score = 500
#     elif sub == "humansbeingbros":
#         required_score = 500

#     posts = api.search_submissions(subreddit={sub}, score={required_score}, limit=10)
    
#     postlist = []

#     for post in posts:
#         if post['author_fullname'] != None:
#             try:
#                 if debug: 
#                     print(post)
#                 postlist.append({
#                 "id": post['id'],
#                 "title": cleanString(post['title']),
#                 "author": post['author_fullname'],
#                 "score": post['score'],
#                 "upvote_ratio": post['upvote_ratio'],
#                 "num_comments": post['num_comments'],
#                 "created_utc": str(datetime.fromtimestamp(int(post['created_utc'])).strftime('%Y-%m-%d %H:%M:%S')).strip(),
#                 "flair": post['link_flair_text'],
#                 "is_original_content": post['is_original_content'],
#                 "is_self": post['is_self'],
#                 "over_18": post['over_18'],
#                 "stickied": post['stickied'],
#                 "permalink": "https://reddit.com" + post['permalink'],
#                 })
                
#             except Exception as e:
#                 print(e)
#         return
#     return postlist