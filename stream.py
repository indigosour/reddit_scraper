import praw
from datetime import datetime
from sqlalchemy import create_engine, Column, Integer, String, DECIMAL, DateTime, Boolean, LargeBinary
import sqlalchemy.orm
from sqlalchemy.orm import sessionmaker
from sqlalchemy.exc import IntegrityError
from common import *

db_name = 'reddit_scraper'
database_url = f"mysql+pymysql://{get_az_secret('DB-CRED')['username']}:{get_az_secret('DB-CRED')['password']}@{get_az_secret('DB-CRED')['url']}:3306/{db_name}"
Base = sqlalchemy.orm.declarative_base()
engine = create_engine(database_url)

def create_sqlalchemy_session():
    Session = sessionmaker(bind=engine)
    session = Session()
    return session

class Post(Base):
    __tablename__ = 'posts'

    post_id = Column(String(length=8), primary_key=True)
    title = Column(String(length=500))
    author = Column(String(length=255))
    subreddit = Column(String(length=255))
    score = Column(Integer)
    upvote_ratio = Column(DECIMAL(precision=5, scale=4))
    num_comments = Column(Integer)
    created_utc = Column(DateTime)
    last_updated = Column(DateTime)
    videohash = Column(LargeBinary)
    is_downloaded = Column(Boolean)
    permalink = Column(String(length=255))
    is_original_content = Column(Boolean)
    over_18 = Column(Boolean)

    def __repr__(self):
        return f"<Post(id='{self.id}', title='{self.title}')>"


class Inventory(Base):
    __tablename__ = 'inventory'

    post_id = Column(Integer, primary_key=True)
    last_updated = Column(DateTime)
    watched = Column(Boolean)
    tube_id = Column(String(length=255))

    def __repr__(self):
        return f"<Inventory(id='{self.post_id}')>"


def create_table(table_name=None):
    Base.metadata.create_all(engine, tables=[Base.metadata.tables[table_name]]) if table_name else Base.metadata.create_all(engine)

def drop_table(table_name=None):
    Base.metadata.drop_all(engine, tables=[Base.metadata.tables[table_name]]) if table_name else Base.metadata.drop_all(engine)

def stream_and_store_posts():
    reddit = reddit_auth()
    sublist = load_sublist()
    session = create_sqlalchemy_session()
    drop_table('posts')
    create_table('posts')
    subreddit_list = "+".join(sublist)
    for submission in reddit.subreddit(subreddit_list).stream.submissions():
        post = Post(
            id=submission.id,
            title=cleanString(submission.title)[:500],
            author=submission.author.name,
            subreddit=submission.subreddit,
            score=submission.score,
            upvote_ratio=submission.upvote_ratio,
            num_comments=submission.num_comments,
            created_utc=datetime.fromtimestamp(int(submission.created_utc)),
            is_original_content=submission.is_original_content,
            over_18=submission.over_18,
            permalink="https://reddit.com" + submission.permalink,
            last_updated=datetime.now()
        )
        try:
            session.add(post)
            session.commit()
            print(f"Added post {submission.id}")
        except IntegrityError:
            session.rollback()
            continue