from sqlalchemy import create_engine, Column, Integer, String, LargeBinary, Boolean, DateTime, text, DECIMAL
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm
import logging, datetime, logging
from datetime import timedelta
from azvault import get_az_secret
from main import cleanString, load_sublist

database_url = f"mysql+pymysql://{get_az_secret('DB-CRED')['username']}:{get_az_secret('DB-CRED')['password']}@{get_az_secret('DB-CRED')['url']}:3306/reddit_scraper_dev"


def create_sqlalchemy_session():
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

Base = sqlalchemy.orm.declarative_base()


def create_subreddit_class(sub):
    class Subreddit(Base):
        __tablename__ = f'subreddit_{sub}'
        id = Column(String(255), primary_key=True)
        title = Column(String(255))
        author = Column(String(255))
        score = Column(Integer)
        upvote_ratio = Column(DECIMAL)
        num_comments = Column(Integer)
        created_utc = Column(DateTime)
        flair = Column(String(255))
        is_original_content = Column(Boolean)
        is_self = Column(Boolean)
        over_18 = Column(Boolean)
        stickied = Column(Boolean)
        permalink = Column(String(255))
        path = Column(String(255))
        videohash = Column(LargeBinary(66))
        is_downloaded = Column(Boolean)
    
    return Subreddit


def create_sub_table(sub):
    session = create_sqlalchemy_session()
    try:
        Subreddit = create_subreddit_class(sub)
        Subreddit.__table__.create(bind=session.get_bind(), checkfirst=True)
        Base.metadata.remove(Subreddit.__table__)
        print(f'Successfully created table subreddit_{sub} in the db')
        logging.info(f'create_sub_table: Successfully created table subreddit_{sub} in the db')
    except Exception as e:
        print("Error creating table", e)
        logging.error(f'create_sub_table: Error creating table subreddit_{sub} in the db.')
    finally:
        session.close()

def drop_sub_table(sub):
    session = create_sqlalchemy_session()
    try:
        Subreddit = create_subreddit_class(sub)
        Subreddit.__table__.drop(session.get_bind(), checkfirst=True)
        Base.metadata.remove(Subreddit.__table__)
        print(f'Successfully dropped table subreddit_{sub} from the db')
        logging.info(f'drop_sub_table: Successfully dropped table subreddit_{sub} from the db')
    except Exception as e:
        print("Error dropping table", e)
        logging.error(f'drop_sub_table: Error dropping table subreddit_{sub} from the db.')
    finally:
        session.close()

def store_reddit_posts(sub, postlist):
    session = create_sqlalchemy_session()
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
            session.add(create_subreddit_class(sub)(id=id,title=title,author=author,score=score,upvote_ratio=upvote_ratio,
            num_comments=num_comments,created_utc=created_utc,flair=flair,is_original_content=is_original_content,
            is_self=is_self,over_18=over_18,stickied=stickied,permalink=permalink))
            session.commit()
            entrycount+=1
        except Exception as e:
            if "Duplicate" not in str(e):
                print("Error inserting data into table", e)
                logging.error("store_reddit_posts: Error inserting data", e)
            session.rollback()
            continue
    logging.info(f"store_reddit_posts: Completed adding posts for {sub} to database.")
    session.close()
    return print(f"Successfully added {entrycount} entries to database")


def get_dl_list_period(sub,period):
    session = create_sqlalchemy_session()
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
            query = text(f"""
            SELECT title, permalink, id FROM subreddit_{sub} WHERE created_utc BETWEEN :start_date AND :end_date AND score > 500
            """)
            dl_list = session.execute(query, {'start_date': start_date, 'end_date': end_date}).fetchall()
        except Exception as e:
            print("Error reading data from table", e)
            logging.error("get_dl_list_period: Error reading data from table", e)
        finally:
            session.close()
    else:
        print("Invalid subreddit selected")

    return dl_list


def update_item_db(sub, v_hash, id, vid_uuid):
    session = create_sqlalchemy_session()
    try:
        query = text(f"""
        UPDATE subreddit_{sub} SET videohash = :v_hash, path = :vid_uuid WHERE id = :id
        """)
        session.execute(query, {'v_hash': v_hash, 'vid_uuid': vid_uuid, 'id': id})
        session.commit()
        print(session.rowcount, "record(s) updated")
    except Exception as e:
        print("Error inserting data into table", e)
        logging.error(f'update_item_db: Error inserting data into table', e)
    finally:
        session.close()