import prawcore,time
from redvid import Downloader
from common import *


def get_reddit_posts(sub, period):
    retry_count = 0
    max_retries = 5

    while retry_count < max_retries:
        try:
            with reddit_auth() as reddit:
                posts = reddit.subreddit(sub).top(
                    time_filter=period,
                    limit=1000
                )
            break  # If the request is successful, break out of the loop
        except prawcore.exceptions.ResponseException as e:
            if e.response.status_code == 503:
                print(f"Caught a 503 error: {e}.")
                
                # Print response headers
                print("Response headers:", e.response.headers)
                
                # Check for 'retry_after' header
                retry_after = int(e.response.headers.get('retry-after', 5))
                print(f"Retrying in {retry_after} seconds...")
                
                time.sleep(retry_after)
                retry_count += 1
            else:
                raise
        except Exception as e:
            raise

    if retry_count == max_retries:
        print("Failed to complete the request after maximum retries")
        return None

    return posts


# Download video posts

def download_video(url, path):
    try:
        reddit = Downloader(max_q=True)
        reddit.url = url
        reddit.path = path
        reddit.download()
    except BaseException as e:
        if str(e) == 'No video in this post':
            print("No video found in this post. Skipping...")
        else:
            raise
