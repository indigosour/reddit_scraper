import mysql.connector as database
import praw, datetime
from videohash import VideoHash
debug = False

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

username = "dbuser"
password = "***REMOVED***"
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

