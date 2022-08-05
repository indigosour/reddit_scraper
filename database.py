import mysql.connector as database
import names, praw, datetime

reddit_read_only = praw.Reddit(client_id="uM6URp2opqPfANoCdPE09g",         # your client id
                               client_secret="ofL3-C58gmXaHgiGHYJ_Mx4MdmOd3w",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

username = "dbuser"
password = "Pbp5zSfVEZcLTr8skxQbVYgJ3YpLBE"

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
            PostID int NOT NULL AUTO_INCREMENT,
            Title varchar(255),
            Score int,
            upvote_ratio DECIMAL,
            created_utc DATETIME,
            PRIMARY KEY (PersonID)
                );
        
        """
        cursor.execute(statement)
        connection.commit()
        print("Successfully created table in the db")
    except database.Error as e:
        print(f"Error adding entry to database: {e}")


def add_data(first_name, last_name):
    try:
        statement = "INSERT INTO employees (FirstName,LastName) VALUES (%s, %s)"
        data = (first_name, last_name)
        cursor.execute(statement, data)
        connection.commit()
        print("Successfully added entry to database")
    except database.Error as e:
        print(f"Error adding entry to database: {e}")

def get_data(last_name):
    try:
      statement = "SELECT FirstName, LastName FROM employees WHERE LastName=%s"
      data = (last_name,)
      cursor.execute(statement, data)
      for (first_name, last_name) in cursor:
        print(f"Successfully retrieved {first_name}, {last_name}")
    except database.Error as e:
      print(f"Error retrieving entry from database: {e}")


def get_reddit_list_number(sub,num):
    posts = reddit_read_only.subreddit(f'{sub}').top(limit=num)
    postlist = []
    for post in posts:
        postlist.append({
        "id": post.id,
        "title": post.title,
        "author": post.author.name,
        "score": post.score,
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
            
            INSERT INTO subreddit_{sub} (id,title,author,score,num_comments,created_utc,
            permalink,link_flair_template_id,flair,is_original_content,is_self,over_18,stickied) 
            VALUES ({id},{title},{author},{score},{num_comments},{created_utc},
            {permalink},{link_flair_template_id},{flair},{is_original_content},{is_self},{over_18},{stickied})"
            
            """
            cursor.execute(statement)
            connection.commit()
            print("Successfully added entry to database")
            ++entrycount
        except database.Error as e:
            print(f"Error adding entry to database: {e}")
         

    return print(f"Successfully added {entrycount} entries to database")
    