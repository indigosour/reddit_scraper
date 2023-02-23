import mysql.connector as db
import os,time,praw,glob,requests,emoji,re,json,logging
from videohash import VideoHash
from redvid import Downloader
from datetime import datetime, timedelta

logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.info)


reddit = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

# Variables and config

working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"
peertube_api_url = "https://tubelrone.com/api/v1"
AZURE_STORAGE_ACCOUNT_NAME = "tubelrone"
AZURE_STORAGE_CONTAINER_NAME = "appstorage"
peertube_token = None

sublist = [

            "unexpected",
            "funny",
            "whatcouldgowrong",
            "humansbeingbros",
            "contagiouslaughter",
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

channel_list = {

                "unexpected":"1",
                "funny":"2",
                "whatcouldgowrong":"3"
                
            }


def cleanString(sourcestring):
    text_ascii = emoji.demojize(sourcestring) if sourcestring else ""
    pattern = r"[%:/,.\"\\[\]<>*\?]"
    text_without_emoji = re.sub(pattern, '', text_ascii) if text_ascii else ""
    return text_without_emoji


######################
######## SQL #########
######################

def create_db_connection():
    connection = None
    user_name = "python_u@sapphiretube"
    user_password = "3VtirKSv5SNRmm3bhmLm"
    host_name = "sapphiretube.mariadb.database.azure.com"
    db_name = "reddit_scraper"
    try:
        connection = db.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        logging.info("create_db_connection: Connecting to DB...")
    except db.Error as err:
        print(f"Error: '{err}'")
        logging.error(f"create_db_connection: Error opening DB connection: '{err}'")
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
        logging.info(f'create_sub_table: Successfully created table subreddit_{sub} in the db')
        cursor.close()
    except db.Error as e:
        print("Error creating table", e)
        logging.error(f'create_sub_table: Error creating table subreddit_{sub} in the db.')
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
        logging.info('drop_table: {table} dropped successfully')
    except db.Error as e:
        print("Error dropping table", e)
        logging.error('drop_table: Error dropping table', e)
    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()


##### Store reddit posts in DB ######
# Todo
# - Add the ability to update existing entries with the latest score

def store_reddit_posts(sub, postlist):
    connection = create_db_connection()
    cursor = connection.cursor()
    entrycount = 0
    logging.info(f"store_reddit_posts: Storing reddit posts in subreddit_{sub}") 
    for post in postlist:
        id = post['id']
        title = cleanString(post['title'])
        author = post['author']
        score = post['score']
        upvote_ratio = post['upvote_ratio']
        num_comments = post['num_comments']
        created_utc = post['created_utc']
        flair = cleanString(post['flair'])
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
            logging.debug(f'store_reddit_posts: Attempting to comit {statement}')
            cursor.execute(statement)
            connection.commit()
            #print("Successfully added entry to database")
            entrycount+=1
        except db.Error as e:
            if e.errno != 1062:
                print("Error inserting data into table", e)
                logging.error("store_reddit_posts: Error inserting data", e)
            continue
    logging.info(f"store_reddit_posts: Completed adding posts for {sub} to database.")
    if connection.is_connected():
        connection.close()
        cursor.close()
    return print(f"Successfully added {entrycount} entries to database")


def get_dl_list_period(sub,period):
    connection = create_db_connection()
    cursor = connection.cursor()
    logging.info(f"get_dl_list_period: Getting download list for subreddit {sub} for the period {period} ")

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

    logging.info(f"get_dl_list_period: Period calculated \nStart Date: {start_date} and End Date: {end_date}")

    try:
        select_Query = """
        SELECT title, permalink, id FROM subreddit_%s
        WHERE created_utc BETWEEN %s AND %s AND score > 500
        """
        cursor.execute(select_Query, (sub, start_date, end_date))
        dl_list = cursor.fetchall()

        # for post in dl_list:
        #     print(post[0])
        #     print(post[1])

    except db.Error as e:
        print("Error reading data from table", e)
        logging.error("get_dl_list_period: Error reading data from table", e)

    finally:
        if connection.is_connected():
            connection.close()
            cursor.close()
    return dl_list


def update_item_db(sub,v_hash,id,vid_uuid):
    connection = create_db_connection()
    cursor = connection.cursor()
    try:
        sql = "UPDATE subreddit_{} SET videohash = '{}', path = '{}' WHERE id = '{}'".format(sub,v_hash,vid_uuid,id)
        #print(sql)

        cursor.execute(sql)

        connection.commit()

        print(cursor.rowcount, "record(s) updated")
    except db.Error as e:
        print("Error inserting data into table", e)
        logging.error(f'update_item_db: Error inserting data into table',e)

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
    logging.info(f'get_reddit_list: Getting list for {sub} for {period}')
    postlist = []
    
    for post in posts:
        if post.author != None and post.is_video == True:
            try:
                logging.debug(f'get_reddit_list: {post}')
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
                logging.error(f'get_reddit_list: Error getting post.',e)
                print(e)
    
    if postlist > 10:
        logging.info(f'get_reddit_list: Successfully retrieved reddit list for {sub} for period {period}')
    else:
        logging.error(f'get_reddit_list: Failure to retrieve reddit list for {sub} for period {period}')
    return postlist


# Download video posts

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()

######################
##### Peertube #######
######################

def peertube_auth():
    global peertube_token
    peertube_api_user = "autoupload1"
    peertube_api_pass = "BfWnTPT#Z@vF%Y6BPV%8xMjp%7vKS5"
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


###########################
##### MAIN FUNCTIONS ######
###########################

# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_dl_period(sub, period):
    try:
        global working_dir

        # Get list of posts for download
        dlList = get_dl_list_period(f'{sub}', f'{period}')
        today = datetime.today().strftime('%m-%d-%Y')

        # Run peertube auth to get token for this session
        peertube_auth()
        
        logging.info(f'main_dl_period: Beginning main_dl_period creating playlist for {sub} and period {period}.')
        
        # Create playlist for session
        playlist_id = create_playlist(f'{sub} top of the {period} - Generated {today}', sub)

        # Download each video post
        logging.info(f'main_dl_period: Downloading video posts for for {sub} and period {period}.')
        for post in dlList:
            logging.debug(post[1])
            print(f"Downloading {dlList.__len__} posts.")
            sani_title = cleanString(post[0])
            id = post[2]
            url = str(post[1])

            # Download video and store in working directory
            try:
                download_video(url, working_dir)
            except Exception as e:
                print(f"Error downloading video: {e}")
                logging.error(f"main_dl_period: Error downloading video: {e}")
                continue
            time.sleep(0.500)

            working_file = glob.glob(f"{working_dir}/*.mp4")[0]

            # Generate video hash
            try:
                v_hash = VideoHash(path=working_file)
                hash_value = v_hash.hash
                logging.debug(f'get video hash: Video hash is {hash_value}')
            except Exception as e:
                print(f"Error generating video hash: {e}")
                logging.error(f"main_dl_period: Error generating video hash: {e}")
                continue


            # Upload video to peertube
            try:
                vid_uuid = upload_video(sub, sani_title, working_file)
            except Exception as e:
                print(f"Error uploading video: {e}")
                logging.error(f"main_dl_period: Error uploading video: {e}")
                continue

            # Add video hash and path to database
            try:
                update_item_db(sub, hash_value, id, vid_uuid)
            except Exception as e:
                print(f"Error updating database: {e}")
                logging.error("main_dl_period: Error updating database: {e}")
                continue

            # Add video to same playlist
            try:
                add_video_playlist(vid_uuid, playlist_id)
            except Exception as e:
                print(f"Error adding video to playlist: {e}")
                logging.error(f'main_dl_period: Error adding video to playlist: {e}')
                continue
            
            # Remove uploaded video
            try:
                os.remove(working_file)
            except Exception as e:
                print("Error removeing file: {e}")
                logging.error(f'main_dl_period: Error removing file: {e}')

        print("Completed downloading videos")

        # Cleanup working directory
        cleanup_workingdir()
        logging.info("Cleaned up working directory")
        logging.info(f'main_dl_period: Completed for {sub} and period {period}.')

    except Exception as e:
        print(f"Error occurred: {e}")
    except KeyboardInterrupt:
        print('Interrupted')


def cleanup_workingdir():
    working_folder_list = glob.glob(f"{working_dir}/*.mp4")
    for i in working_folder_list:
        os.remove(i)
    return print("Cleaned up working directory")


def update_DB():
    global sublist
    for sub in sublist:
        #drop_table(f"subreddit_{sub}")
        #create_sub_table(f"{sub}")
        #print(f"Table {sub} created moving on to download posts")
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
        print(f'Completed downloading {sub}')


def main():
    while True:
        print("Welcome to Reddit Scraper! \nPlease select an option below:")
        print("1. Gather a specific subreddit for a period. (Eg. week, month, etc.)")
        print("2. Gather all subreddits for a specific period. (Eg. week, month, etc.)")
        print("3. Update the databases with the latest posts.")
        print("4. Exit the program")

        choice = input("Enter your choice: ")

        if choice == "1":
            choice_sub = input("Which subreddit would you like to download?")
            print(f"You selected option 1. \nNow gathering and downloading posts from {choice_sub}...")
            


        elif choice == "2":

            print("You selected option 2. \nNow gathering and downloading posts...")

        elif choice == "3":

            print("You selected option 3. \nBeginning db update now...")

        elif choice == "4":

            print("Exiting...")
            break

        else:
            print("Invalid choice. Please try again.")

if __name__ == "__main__":
    main()

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