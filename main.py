import os,time,praw,glob,logging,shutil
from videohash import VideoHash
from redvid import Downloader
from datetime import datetime
from database import *
from peertube import *
from common import *


logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.INFO)

# Variables and config

working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"


# Download video posts

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()

# Cleanup working directory of files and folders

def cleanup_workingdir():
    working_folder_list = glob.glob(f"{working_dir}/*")
    for i in working_folder_list:
        shutil.rmtree(i,True)
    working_folder_list = glob.glob(f"{working_dir}/*")
    for i in working_folder_list:
        os.remove(i)
    return print("Cleaned up working directory")


###########################
##### MAIN FUNCTIONS ######
###########################

# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_dl_period(sub,period,playlist_id):
    try:
        global working_dir
        # Get list of posts for download
        dlList = get_dl_list_period(sub,period)

        # Run peertube auth to get token for this session
        peertube_auth()

        logging.info(f'main_dl_period: Beginning main_dl_period creating playlist for {sub} and period {period}.')
        
        # Download each video post
        logging.info(f'main_dl_period: Downloading video posts for for {sub} and period {period}.')
        
        if len(dlList) > 0:
            print(f"Downloading {len(dlList)} posts.")

            for post in dlList:
                logging.debug(post['permalink'])
                
                sani_title = cleanString(post['title'])
                id = post['id']
                url = str(post['permalink'])

                # Download video and store in working directory
                try:
                    download_video(url, working_dir)
                except Exception as e:
                    print(f"Error downloading video: {e}")
                    logging.error(f"main_dl_period: Error downloading video: {e}")
                    break
                time.sleep(0.500)

                working_file = glob.glob(f"{working_dir}/*.mp4")[0]

                # Generate video hash
                try:
                    v_hash = VideoHash(path=working_file)
                    hash_value = v_hash.hash
                    binary_data = int(hash_value, 2).to_bytes((len(hash_value) - 2) // 8 + 1, 'big')
                    logging.debug(f'get video hash: Video hash is {binary_data}')
                except Exception as e:
                    print(f"Error generating video hash: {e}")
                    logging.error(f"main_dl_period: Error generating video hash: {e}")
                    break

                # Upload video to peertube
                try:
                    vid_uuid = upload_video(sub, sani_title, working_file)
                except Exception as e:
                    print(f"Error uploading video: {e}")
                    logging.error(f"main_dl_period: Error uploading video: {e}")
                    break

                # Add video hash and path to database
                try:
                    insert_inventory(binary_data, id, vid_uuid)
                    print(f'{sub} {binary_data} {id} {vid_uuid}')
                except Exception as e:
                    print(f"Error updating database: {e}")
                    logging.error("main_dl_period: Error updating database: {e}")
                    break

                # Add video to playlist p_id
                try:
                    add_video_playlist(vid_uuid, playlist_id)
                except Exception as e:
                    print(f"Error adding video to playlist: {e}")
                    logging.error(f'main_dl_period: Error adding video to playlist: {e}')
                    break
                
                # Remove uploaded video
                try:
                    os.remove(working_file)
                except Exception as e:
                    print("Error removeing file: {e}")
                    logging.error(f'main_dl_period: Error removing file: {e}')

            print("Completed downloading videos")

            # Cleanup working directory
            cleanup_workingdir()
            logging.info("Cleaned up working directory")
            logging.info(f'main_dl_period: Completed for {sub} and period {period}.')
        else:
            print("No posts returned for download")

    except Exception as e:
        print(f"Error occurred: {e}")
    except KeyboardInterrupt:
        print('Interrupted')


def grab_dat(period):
    sublist = load_sublist()
    today = datetime.today().strftime('%m-%d-%Y')
    peertube_auth()
    p_id = create_playlist(f'Top of the {period} for all subs as of {today}', 2)
    for sub in sublist: 
        main_dl_period(sub,period,p_id)
        cleanup_workingdir()
        print(f'Completed downloading {sub}')


# def main():
#     sublist = load_sublist()
#     choicelist = {}
#     today = datetime.today().strftime('%m-%d-%Y')
#     while True:
#         print("Welcome to Reddit Scraper! \nPlease select an option below:")
#         print("1. Gather a specific subreddit for a period. (Eg. week, month, etc.)")
#         print("2. Gather all subreddits for a specific period. (Eg. week, month, etc.)")
#         print("3. Update the databases with the latest posts.")
#         print("4. Exit the program")

#         choice = input("Enter your choice: ")

#         if choice == "1":
#             num = 1
#             print("You selected option 1.")
#             print("For which period would you like to download? \nPlease select an option below: ")
#             print("1. Top of the last Day")
#             print('2. Top of the last Week')
#             print('3. Top of the last Month')
#             print('4. Top of the last Year')
            
#             choice_period = input("Enter your choice: ")
            
#             if choice_period == '1':
#                 print("You've chosen day.")
#                 chosen_period = 'day'
#             if choice_period == '2':
#                 print("You've chosen week.")
#                 chosen_period = 'week'
#             if choice_period == '3':
#                 print("You've chosen month.")
#                 chosen_period = 'month'
#             if choice_period == '4':
#                 print("You've chosen year.")
#                 chosen_period = 'year'

#             choicelist = {
#                     'num': 'sub' 
#                 }
#             print(f"Download the top of {chosen_period} for which subreddit? \nSelect an option below: ")

#             for sub in sublist:
#                 print(f'Choice {num}: {sub}')
#                 choicelist[f'{num}']=sub
#                 num = num + 1
            
#             choice_sub = input("Enter your choice: ")
            
#             chosen_sub = choicelist[choice_sub]

#             if chosen_sub is None:
#                 print("Invalid choice")  
#             elif True:
#                 print(f'You\'ve chosen option {choice_sub}: {chosen_sub}')
#             print(f"Now gathering and downloading posts from {chosen_sub}...")
            
#             try:
#                 p_id = create_playlist(f'{chosen_sub} top of the {chosen_period} - Generated {today}', chosen_sub)
#                 main_dl_period(chosen_sub,chosen_period,p_id)
#             except KeyboardInterrupt:
#                 print("KeyboardInterrupted!")
#                 break

#         elif choice == "2":
#             print("You selected option 2. Download all subreddits for a specific period of time.")
#             print("For which period would you like to download? \nPlease select an option below: ")
#             print("1. Top of the last Day")
#             print('2. Top of the last Week')
#             print('3. Top of the last Month')
#             print('4. Top of the last Year')
            
#             choice_period = input("Enter your choice: ")
            
#             if choice_period == '1':
#                 print("You've chosen day.")
#                 chosen_period = 'day'
#             if choice_period == '2':
#                 print("You've chosen week.")
#                 chosen_period = 'week'
#             if choice_period == '3':
#                 print("You've chosen month.")
#                 chosen_period = 'month'
#             if choice_period == '4':
#                 print("You've chosen year.")
#                 chosen_period = 'year'
    
#             try:
#                 grab_dat(chosen_period)
#             except KeyboardInterrupt:
#                 print("KeyboardInterrupted!")
#                 break

#         elif choice == "3":
#             print("You selected option 3. \nBeginning db update now...")
#             try:
#                 update_DB()
#             except Exception as e:
#                 print("Error updating the DB: %s" % e)
#             except KeyboardInterrupt:
#                 print("KeyboardInterrupt!")
#                 break

#         elif choice == "4":

#             print("Exiting...")
#             break

#         else:
#             print("Invalid choice. Please try again.")

# if __name__ == "__main__":
#     main()