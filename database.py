import mysql.connector as database
import praw, datetime, requests
from videohash import VideoHash
debug = False

reddit_read_only = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

username = "dbuser"
password = "GxVpw3zwBYx7eJ8uX2jW844du4Bc2m"
connection = database.connect(
    user=username,
    password=password,
    host="mongo1.thisjayjay.gmail.com.beta.tailscale.net",
    database="reddit_scraper"
    )


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

def create_sub_table(sub):
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
            PRIMARY KEY (id)
                );
        
        """
        cursor.execute(statement)
        connection.commit()
        print(f'Successfully created table subreddit_{sub} in the db')
        cursor.close()
    except database.Error as e:
        print(f"Error adding entry to database: {e}")
        cursor.close()



def drop_table(table):
    cursor = connection.cursor()
    try:
        cursor.execute(f"DROP TABLE IF EXISTS {table}")
        connection.commit()
        print(f'{table} dropped')
    except database.Error as e:
        cursor.close()
        print(f"Error adding entry to database: {e}")
    cursor.close()


def get_reddit_list_number(sub,num):
    posts = reddit_read_only.subreddit(f'{sub}').top(limit=num)
    postlist = []
    for post in posts:
        if post.author != None and post.score > 1000:

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
                "created_utc": str(datetime.datetime.fromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S')).strip(),
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

def store_reddit_posts(sub, postlist):
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
            created_utc,flair,is_original_content,is_self,over_18,stickied,permalink,path) 
            VALUES ("{id}","{title}","{author}",{score},{upvote_ratio},{num_comments},"{created_utc}",
            "{flair}",{is_original_content},{is_self},{over_18},{stickied},"{permalink}",NULL)
            
            """
            #print(statement)
            cursor.execute(statement)
            connection.commit()
            print("Successfully added entry to database")
            entrycount+=1
        except database.Error as e:
            cursor.close()
            print(f"Error adding entry to database: {e}")
    cursor.close()
    return print(f"Successfully added {entrycount} entries to database")


def main(sub,num):
    postlist = get_reddit_list_number(sub,num)
    store_reddit_posts(sub,postlist)

def grab_dat():
    global sublist
    for sub in sublist:
        drop_table(f"subreddit_{sub}")
        create_sub_table(f"{sub}")
        main(sub,500)

