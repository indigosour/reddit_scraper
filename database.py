from sqlalchemy import create_engine,Column,Integer,String,LargeBinary,Boolean,DateTime,DECIMAL,Table,MetaData
from sqlalchemy.orm import sessionmaker
import sqlalchemy.orm
import logging,datetime
from datetime import timedelta, datetime
from common import *

db_name = 'reddit_scraper'
database_url = f"mysql+pymysql://{get_az_secret('DB-CRED')['username']}:{get_az_secret('DB-CRED')['password']}@{get_az_secret('DB-CRED')['url']}:3306/{db_name}"
Base = sqlalchemy.orm.declarative_base()


def create_sqlalchemy_session():
    engine = create_engine(database_url)
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

def reflect_table_metadata(engine, sub):
    metadata = MetaData()
    try:
        metadata.reflect(bind=engine, only=[f'subreddit_{sub}'])
    except Exception:
        pass  # ignore if table does not exist
    return metadata


def create_subreddit_table(connection, metadata, sub):
    inspector = sqlalchemy.inspect(connection)
    table_name = f'subreddit_{sub}'

    if inspector.has_table(table_name):
        return metadata.tables[table_name]

    return Table(
        table_name,
        metadata,
        Column('id', String(255), primary_key=True),
        Column('title', String(255)),
        Column('author', String(255)),
        Column('score', Integer),
        Column('upvote_ratio', DECIMAL),
        Column('num_comments', Integer),
        Column('created_utc', DateTime),
        Column('flair', String(255)),
        Column('is_original_content', Boolean),
        Column('is_self', Boolean),
        Column('over_18', Boolean),
        Column('stickied', Boolean),
        Column('permalink', String(255)),
        Column('path', String(255)),
        Column('videohash', LargeBinary()),
        Column('is_downloaded', Boolean)
    )


def create_subreddit_class(connection, metadata, sub):
    subreddit_table = create_subreddit_table(connection, metadata, sub)
    class Subreddit(Base):
        __table__ = subreddit_table
    return Subreddit


def create_sub_table(sub):
    engine = create_engine(database_url)
    metadata = reflect_table_metadata(engine, sub)
    try:
        with engine.connect() as connection:
            Subreddit = create_subreddit_class(connection, metadata, sub)
            subreddit_table = Subreddit.__table__
            if not sqlalchemy.inspect(connection).has_table(f'subreddit_{sub}'):
                subreddit_table.create(bind=connection, checkfirst=True)
                Base.metadata.remove(subreddit_table)
                print(f'Successfully created table {subreddit_table.name} in the db')
                logging.info(f'create_sub_table: Successfully created table {subreddit_table.name} in the db')
            else:
                print(f'Table {subreddit_table.name} already exists in the db')
                logging.info(f'create_sub_table: Table {subreddit_table.name} already exists in the db')
    except sqlalchemy.exc.OperationalError as e:
        msg = f"Error creating table subreddit_{sub} in the db: {str(e)}."
        print(msg)
        logging.exception(f'create_sub_table: {msg}')
    except Exception as e:
        msg = f"Error creating table subreddit_{sub} in the db: {str(e)}."
        print(msg)
        logging.error(f'create_sub_table: {msg}')
    finally:
        engine.dispose()


def drop_sub_table(sub):
    engine = create_engine(database_url)
    table_name = f'subreddit_{sub}'
    try:
        with engine.connect() as connection:
            if sqlalchemy.inspect(connection).has_table(table_name):
                metadata = MetaData()
                metadata.reflect(bind=connection, only=[table_name])
                subreddit_table = metadata.tables[table_name]
                subreddit_table.drop(bind=connection)
                print(f'Successfully dropped table {table_name} from the db')
                logging.info(f'drop_sub_table: Successfully dropped table {table_name} from the db')
            else:
                print(f'Table {table_name} does not exist in the db')
                logging.info(f'drop_sub_table: Table {table_name} does not exist in the db')
    except sqlalchemy.exc.OperationalError as e:
        msg = f"Error dropping table {table_name} from the db: {str(e)}."
        print(msg)
        logging.exception(f'drop_sub_table: {msg}')
    except Exception as e:
        msg = f"Error dropping table {table_name} from the db: {str(e)}."
        print(msg)
        logging.error(f'drop_sub_table: {msg}')
    finally:
        engine.dispose()


def store_reddit_posts(sub, postlist):
    session = create_sqlalchemy_session()
    entrycount = 0
    logging.info(f"store_reddit_posts: Storing reddit posts in subreddit_{sub}")
    metadata = reflect_table_metadata(session.bind, sub)

    for post in postlist:
        id = post['id']
        title = cleanString(post['title'][:255])
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
            Subreddit = create_subreddit_class(session.bind, metadata, sub)
            session.add(Subreddit(id=id, title=title, author=author, score=score, upvote_ratio=upvote_ratio,
                                   num_comments=num_comments, created_utc=created_utc, flair=flair,
                                   is_original_content=is_original_content, is_self=is_self, over_18=over_18,
                                   stickied=stickied, permalink=permalink))
            session.commit()
            entrycount += 1
        except Exception as e:
            if "Duplicate" not in str(e):
                print("Error inserting data into table", e)
                logging.error("store_reddit_posts: Error inserting data", e)
            session.rollback()
            continue
    logging.info(f"store_reddit_posts: Completed adding posts for {sub} to database.")
    session.close()
    return print(f"Successfully added {entrycount} entries to database")


def get_dl_list_period(sub, period):
    engine = create_engine(database_url)
    sublist = load_sublist()
    metadata = reflect_table_metadata(engine, sub)
    Subreddit = create_subreddit_class(engine, metadata, sub)

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
            with engine.connect() as connection:
                session = sqlalchemy.orm.Session(bind=connection)
                query = session.query(Subreddit.title, Subreddit.id, Subreddit.permalink).filter(
                    Subreddit.created_utc.between(start_date, end_date), Subreddit.score > 500).all()
                dl_list = [{"title": row.title, "id": row.id, "permalink": row.permalink} for row in query]
                return dl_list
        except Exception as e:
            print("Error reading data from table", e)
            logging.error("get_dl_list_period: Error reading data from table", e)
        finally:
            session.close()
    else:
        print("Invalid subreddit selected")


def update_item_db(sub, v_hash, id, vid_uuid):
    engine = create_engine(database_url)
    metadata = reflect_table_metadata(engine, sub)
    Subreddit = create_subreddit_class(engine, metadata, sub)
    try:
        with engine.connect() as connection:
            result = connection.execute(
                Subreddit.__table__.update().
                where(Subreddit.__table__.c.id == id).
                values(videohash=v_hash, path=vid_uuid))
            print(result.rowcount, "record(s) updated")
    except sqlalchemy.exc.OperationalError as e:
        msg = f"Error updating record in subreddit_{sub}: {str(e)}."
        print(msg)
        logging.exception(f'update_item_db: {msg}')
    except Exception as e:
        msg = f"Error updating record in subreddit_{sub}: {str(e)}."
        print(msg)
        logging.error(f'update_item_db: {msg}')
    finally:
        engine.dispose()