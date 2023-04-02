import os,time,glob,logging,uuid,argparse
from videohash import VideoHash
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from database import *
from peertube import *
from common import *
from reddit import *

# Variables and config
logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.INFO)
logging.getLogger('PIL').setLevel(logging.WARNING)
working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"


###########################
##### MAIN FUNCTIONS ######
###########################

# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_dl_period(period,playlist_id,dlList):
    upload_count = 0
    post_count = len(dlList)
    try:
        global working_dir

        # Create a unique working directory
        working_dir_run = f"{working_dir}/working_{uuid.uuid4()}"
        os.makedirs(working_dir_run)

        # Run peertube auth to get token for this session
        peertube_auth()
        
        # Download each video post
        logging.info(f'main_dl_period: Downloading video posts for period {period}.')
        
        if len(dlList) > 0:
            print(f"Downloading {post_count} posts.")

            for post in dlList:
                logging.debug(post['permalink'])
                
                sani_title = cleanString(post['title'])
                id = post['id']
                sub = post['subreddit']
                url = str(post['permalink'])
                description = f"""
                        Subreddit: {post['subreddit']}
                        Score: {post['score']}
                        URL: {post['permalink']}                        
                        Author: {post['author']}
                        Upvote Ratio: {post['upvote_ratio']}
                        Number of comments: {post['num_comments']}
                        Date Created: {post['created_utc']}

                """
                
                # Check if the post ID exists in inventory table
                #### If it does exist and has not been watched, then add it to the new playlist

                id_check = id_inventory_check(post['id'])

                if len(id_check) > 0:
                    print("Already in downloaded table")
                    if id_check[0][2] == False:
                        v_found_id = id_check[0][1]
                        add_video_playlist(v_found_id, playlist_id)
                    continue
                elif id_check == None:
                    pass

                # Download video and store in working directory
                try:
                    download_video(url, working_dir_run)
                except Exception as e:
                    print(f"Error downloading video: {e}")
                    logging.error(f"main_dl_period: Error downloading video: {e}")
                    continue
                time.sleep(0.500)

                working_file = glob.glob(f"{working_dir_run}/*.mp4")[0]

                # Generate video hash
                try:
                    v_hash = VideoHash(path=working_file)
                    hash_value = v_hash.hash
                    logging.debug(f'get video hash: Video hash is {hash_value}')
                except (Exception,BaseException) as e:
                    print(f"Error generating video hash: {e}")
                    logging.error(f"main_dl_period: Error generating video hash: {e}")
                    continue

                # Dedupe - Check if video hash exists
                hash_check = hash_inventory_check(hash_value)

                if hash_check == True:
                    print(f"main_dl_period: Duplicate video detected and skipped. {post['id']}")
                    os.remove(working_file)
                    logging.info(f"main_dl_period: Duplicate video detected and skipped. {post['id']}")
                    continue
                elif hash_check == False:
                    pass

                # Upload video to peertube
                try:
                    vid_uuid = upload_video(sub,sani_title,working_file,description)
                except Exception as e:
                    print(f"main_dl_period: Error uploading video: {e}")
                    logging.error(f"main_dl_period: Error uploading video: {e}")
                    continue

                # Add video hash and path to database
                try:
                    if vid_uuid != None:
                        insert_inventory(hash_value, id, vid_uuid)
                        logging.debug(f'{hash_value} {id} {vid_uuid}')
                except Exception as e:
                    print(f"Error updating database with video hash: {e}")
                    logging.error("main_dl_period: Error updating database with video hash: {e}")
                    continue

                # Add video to playlist p_id
                try:
                    if vid_uuid != None:
                        add_video_playlist(vid_uuid, playlist_id)
                        upload_count += 1
                except Exception as e:
                    print(f'main_dl_period: Error adding video to peertube playlist {playlist_id}: {e}')
                    logging.error(f'main_dl_period: Error adding video to peertube playlist {playlist_id}: {e}')
                    continue
                
                # Remove uploaded video
                try:
                    os.remove(working_file)
                except Exception as e:
                    print(f'main_dl_period: Error removing file {working_file}: {e}')
                    logging.error(f'main_dl_period: Error removing file {working_file}: {e}')

            print(f"Completed downloading videos, {upload_count}/{post_count} uploaded successfully. \nCleaning up working directory...")
            logging.info(f"main_dl_period: Completed downloading videos, {upload_count}/{post_count} uploaded successfully. \nCleaning up working directory...")

            # Cleanup working directory
            try:
                cleanup_workingdir(working_dir_run)
                print(f"Successfully deleted the folder and its contents: {working_dir_run}")
            except Exception as e:
                print(f"Error deleting the folder and its contents: {e}")

            logging.info("main_dl_period: Cleaned up working directory")
            logging.info(f'main_dl_period: Completed for period {period}.')
        else:
            print("No posts returned for download")

    except Exception as e:
        print(f"Error occurred: {e}")
        cleanup_workingdir(working_dir_run)
    except KeyboardInterrupt:
        cleanup_workingdir(working_dir_run)
        exit(0)


def grab_dat(period, batch_size=100):
    processed_posts = 0
    today = datetime.today().strftime('%m-%d-%Y')
    peertube_auth()
    p_title = f'Top of the {period} for all subs as of {today}'

    # TO DO -
    # Check if playlist exists, if it exists then pass existing p_id

    p_id = create_playlist(p_title, 2)
    dlList = get_dl_list_period(period)

    print(f'Downloading {len(dlList)} posts from {period} for all subreddits.')
    total_posts = len(dlList)

    # Split dlList into batches
    batches = [dlList[i:i + batch_size] for i in range(0, len(dlList), batch_size)]

    # Run main_dl_period for each batch in parallel
    with ThreadPoolExecutor(max_workers=5) as executor:
        for batch in batches:
            try:
                executor.submit(main_dl_period, period, p_id, batch)
                clear_tmp_folder()
            except KeyboardInterrupt:
                exit(0)

    print(f'Completed downloading top of the {period} for all subs. \nCompleted {processed_posts}/{total_posts}')
    logging.info(f'Completed downloading top of the {period} for all subs. \nCompleted {processed_posts}/{total_posts}')


def process_subreddit_update():
    sublist = load_sublist()

    with ThreadPoolExecutor() as executor:
        futures = [executor.submit(store_reddit_posts, sub) for sub in sublist]

        for future in concurrent.futures.as_completed(futures):
            future.result()

    print("Completed updating database with all posts from all tracked subreddits")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run grab_dat or process_subreddit_update from command line")
    parser.add_argument("-g", "--grab_dat", type=str, help="Period to grab the top posts (hour, day, week, month, year, all)")
    parser.add_argument("-p", "--process_subreddit_update", action="store_true", help="Process subreddit update")

    args = parser.parse_args()

    if args.grab_dat:
        grab_dat(args.grab_dat)
    elif args.process_subreddit_update:
        process_subreddit_update()
    else:
        print("Please provide an argument: --grab_dat or --process_subreddit_update")