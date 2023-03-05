import mysql.connector as db
import logging
from datetime import datetime, timedelta
from loadsublist import load_sublist
from azvault import get_az_secret
from cleanString import cleanString


######################
######## SQL #########
######################

def create_db_connection():
    connection = None
    db_cred = get_az_secret("DB-CRED")
    user_name = db_cred['username']
    user_password = db_cred['password']
    host_name = db_cred['url']
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
        title = cleanString(post['title'])[:255]
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
    sublist = load_sublist()
    logging.info(f"get_dl_list_period: Getting download list for subreddit {sub} for the period {period} ")

    # Determine the start and end dates based on the period given
    if period == "day":
        start_date = (datetime.today() - timedelta(days=1)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "week":
        start_date = (datetime.today() - timedelta(days=7)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "month":
        start_date = (datetime.today() - timedelta(days=30)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    elif period == "year":
        start_date = (datetime.today() - timedelta(days=365)).strftime('%Y-%m-%d')
        end_date = datetime.today().strftime('%Y-%m-%d')
    else:
        print("Invalid period")

    logging.info(f"get_dl_list_period: Period calculated \nStart Date: {start_date} and End Date: {end_date}")

    if sub in sublist:
        try:
            select_Query = f"""
            SELECT title, permalink, id FROM subreddit_{sub} WHERE created_utc BETWEEN {start_date} AND {end_date} AND score > 500
            """
            logging.debug(f'get_dl_list_period: {select_Query}')
            cursor.execute(select_Query)
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
    else:
        print("Invalid subreddit selected")

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