import mysql.connector as database
import praw, datetime

reddit_read_only = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

username = "dbuser"
password = "***REMOVED***"

connection = database.connect(
    user=username,
    password=password,
    host="10.0.0.20",
    database="test"
    )

cursor = connection.cursor()

def create_sub_table(sub):
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
            permalink varchar(255),
            link_flair_template_id varchar(255),
            flair varchar(255),
            is_original_content tinyint,
            is_self tinyint,
            over_18 tinyint,
            stickied tinyint,
            PRIMARY KEY (id)
                );
        
        """
        cursor.execute(statement)
        connection.commit()
        print("Successfully created table in the db")
    except database.Error as e:
        print(f"Error adding entry to database: {e}")


def get_reddit_list_number(sub,num):
    posts = reddit_read_only.subreddit(f'{sub}').top(limit=num)
    postlist = []
    for post in posts:
        postlist.append({
        "id": post.id,
        "title": post.title,
        "author": post.author.name,
        "score": post.score,
        "upvote_ratio": post.upvote_ratio,
        "num_comments": post.num_comments,
        "created_utc": datetime.datetime.fromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S'),
        "permalink": "https://reddit.com" + post.permalink,
        "link_flair_template_id": post.link_flair_template_id,
        "flair": post.link_flair_text,
        "is_original_content": post.is_original_content,
        "is_self": post.is_self,
        "over_18": post.over_18,
        "stickied": post.stickied,
        })
    return postlist


def store_reddit_posts(sub, postlist):
    entrycount = 0
    for x in postlist:

        id = x['id']
        title = x['title']
        author = x['author']
        score = x['score']
        upvote_ratio = x['upvote_ratio']
        num_comments = x['num_comments']
        created_utc = x['created_utc']
        permalink = x['permalink']
        link_flair_template_id = x['link_flair_template_id']
        flair = x['flair']
        is_original_content = x['is_original_content']
        is_self = x['is_self']
        over_18 = x['over_18']
        stickied = x['stickied']

        try:
            statement = f"""
            
            INSERT INTO subreddit_{sub} (id,title,author,score,upvote_ratio,num_comments,created_utc,
            permalink,link_flair_template_id,flair,is_original_content,is_self,over_18,stickied) 
            VALUES ({id},{title},{author},{score},{upvote_ratio},{num_comments},{created_utc},
            {permalink},{link_flair_template_id},{flair},{is_original_content},{is_self},{over_18},{stickied})"
            
            """
            cursor.execute(statement)
            connection.commit()
            print("Successfully added entry to database")
            ++entrycount
        except database.Error as e:
            print(f"Error adding entry to database: {e}")
         

    return print(f"Successfully added {entrycount} entries to database")