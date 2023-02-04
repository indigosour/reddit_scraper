import mysql.connector as db
import praw, requests, glob
from videohash import VideoHash
from pathlib import Path
from redvid import Downloader
import os,time
from datetime import datetime, timedelta

debug = False

reddit_read_only = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

def create_db_connection():
    connection = None
    user_name = "python_u"
    user_password = "GxVpw3zwBYx7eJ8uX2jW844du4Bc2m"
    host_name = "172.18.0.2"
    db_name = "reddit_scraper"
    try:
        connection = db.connect(
            host=host_name,
            user=user_name,
            password=user_password,
            database=db_name
        )
        print("MySQL Database connection successful")
    except db.Error as err:
        print(f"Error: '{err}'")

    return connection

# Variables
working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"
storage_dir = (os.path.dirname(os.path.realpath(__file__))) + "/storage"

sublist = [

            "tiktokcringe",     # https://reddit.com/r/tiktokcringe
            "unexpected",       # https://reddit.com/r/unexpected
            "funny",            # https://reddit.com/r/funny
            "whatcouldgowrong", # https://reddit.com/r/whatcouldgowrong
            "eyebleach",        # https://reddit.com/r/eyebleach
            "humansbeingbros"   # https://reddit.com/r/humansbeingbros

        ]

def cleanString(sourcestring,  removestring ="%:/,.\"\\[]<>*?"):
    #remove the undesireable characters
    return ''.join([c for c in sourcestring if c not in removestring])

######################
######## SQL #########
######################

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
            videohash varchar(255),
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
            print("SQL connection is closed")


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
            print("SQL connection is closed")


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
            print("Successfully added entry to database")
            entrycount+=1
        except db.Error as e:
            print("Error inserting data into table", e)
    if connection.is_connected():
        connection.close()
        cursor.close()
        print("SQL connection is closed")
    return print(f"Successfully added {entrycount} entries to database")


def get_dl_list_period(sub,period):
    connection = create_db_connection()
    cursor = connection.cursor()
    num = 10

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
        
        SELECT title,permalink from subreddit_{sub}
        WHERE created_utc BETWEEN "{start_date}" AND "{end_date}"
        LIMIT {num}
        
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
            print("SQL connection is closed")
    return dl_list

def add_videohash_to_db(sub,v_hash,permalink):
    connection = create_db_connection()
    cursor = connection.cursor()
    try:
        sql = "UPDATE subreddit_%s SET videohash = %s WHERE permalink = %s"
        val = ({sub},{v_hash},{permalink})

        cursor.execute(sql, val)

        db.commit()

        print(cursor.rowcount, "record(s) affected")

    except db.Error as e:
        print("Error inserting data into table", e)


def get_reddit_list_number(sub,num):
    posts = reddit_read_only.subreddit(f'{sub}').new(limit=num)
    postlist = []
    for post in posts:
        if post.author != None and post.score > 500:
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


def get_video_posts_num(sub,num):
    headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
    video_urllist = []
    #sub = 'funny'
    #num = 500
    for post in get_reddit_list_number(f'{sub}',num):
        if post["permalink"]:
            if post["permalink"][len(post["permalink"])-1] == '/':
                json_url = post["permalink"][:len(post["permalink"])-1]+'.json'
            else:
                json_url = post["permalink"] + '.json'
            json_response = requests.get(json_url, 
                            headers= headers)
            if json_response.json()[0]['data']['children'][0]['data']['is_video'] == True and json_response.json()[0]['data']['children'][0]['data']['over_18'] == False:
                json_urllist = {
                    "id": json_response.json()[0]['data']['children'][0]['data']['id'],
                    "permalink": post["permalink"],
                    "title": json_response.json()[0]['data']['children'][0]['data']['title']
                    }
                video_urllist.append(json_urllist)
            elif json_response.status_code != 200:
                print("Error Detected, check the URL!!!")
    return video_urllist


# Download video posts

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()


# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_period():
    sub = 'funny'
    period = 'day'
    global working_dir
    today = datetime.today().strftime('%m-%d-%Y')
    dlList = get_dl_list_period(f'{sub}',f'{period}')
    # uuid_value = str(uuid.uuid4())
    # parent_dir = working_dir
    path = f'{storage_dir}/{sub}_{period}_{today}/'
    isExist = os.path.exists(path)
    if not isExist:
        os.mkdir(path)

    for post in dlList:
        print (post[1])
        sani_title = cleanString(post[0])

        # Download video and store in working directory
        download_video(post[1],working_dir)
        time.sleep(0.500)

        # Rename video file
        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_{period}_{today}/{sani_title}.mp4'
        try:
            Path(old_filename[0]).rename(new_filename)
        except:
            continue
        v_hash = VideoHash(path=f"{new_filename}")
        url = str(post[1])
        # Add video hash to database
        add_videohash_to_db(sub,v_hash.hash,url)

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
        sani_title = cleanString(post["title"])
        download_video(post["permalink"],working_dir)
        time.sleep(0.500)
        old_filename = glob.glob(f"{working_dir}/*.mp4")
        new_filename = f'{storage_dir}/{sub}_Top_{num}_{today}/{sani_title}.mp4'
        try:
            Path(old_filename[0]).rename(new_filename)
        except:
            continue
        print(f'Moving {old_filename[0]} to {new_filename}')


def main(sub,num):
    postlist = get_reddit_list_number(sub,num)
    store_reddit_posts(sub,postlist)


def grab_dat():
    global sublist
    for sub in sublist:
        drop_table(f"subreddit_{sub}")
        create_sub_table(f"{sub}")
        main(sub,5000)