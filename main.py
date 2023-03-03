import os,time,praw,glob,emoji,re,logging
from videohash import VideoHash
from redvid import Downloader
from datetime import datetime
from database import *
from peertube import *


logging.basicConfig(filename='log.log', encoding='utf-8', format='%(asctime)s %(message)s', level=logging.debug)


reddit = praw.Reddit(client_id="***REMOVED***",         # your client id
                               client_secret="***REMOVED***",      # your client secret
                               user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/103.0.0.0 Safari/537.36")        # your user agent

# Variables and config

working_dir = (os.path.dirname(os.path.realpath(__file__))) + "/working"

sublist = [

            "unexpected",
            "funny",
            "whatcouldgowrong",
            "humansbeingbros",
            "contagiouslaughter",
            "therewasanattempt",
            "damnthatsinteresting",
            "nextfuckinglevel",
            "oddlysatisfying",
            "animalsbeingbros",
            "funnyanimals",
            "maybemaybemaybe",
            "beamazed",
            "aww",
            "tiktokcringe",
            "blackmagicfuckery",
            "mademesmile",
            "dankvideos",
            "perfectlycutscreams",
            "praisethecameraman",
            "publicfreakout",
            "peoplefuckingdying",
            "yesyesyesyesno",
            "animalsbeingjerks",
            "nonononoyes"

        ]


def cleanString(sourcestring):
    text_ascii = emoji.demojize(sourcestring) if sourcestring else ""
    pattern = r"[%:/,.\"\\[\]<>*\?]"
    text_without_emoji = re.sub(pattern, '', text_ascii) if text_ascii else ""
    return text_without_emoji


######################
####### REDDIT #######
######################

# get_reddit_list: Get reddit posts and store in a list

# Get the requested number of new posts from the subreddit with minimum upvotes required
# and return a post list to upload to the database.

# Submit: Subreddit (sub) and number (num) of posts to get
# Return: Number of posts requested including id, title, author, score, etc.

def get_reddit_list(sub,period):
    num = 1000
    posts = reddit.subreddit(f'{sub}').top(time_filter=f'{period}',limit=num)
    logging.info(f'get_reddit_list: Getting list for {sub} for {period}')
    postlist = []
    
    for post in posts:
        if post.author != None and post.is_video == True:
            try:
                logging.debug(f'get_reddit_list: {post}')
                postlist.append({
                "id": post.id,
                "title": cleanString(post.title),
                "author": post.author.name,
                "score": post.score,
                "upvote_ratio": post.upvote_ratio,
                "num_comments": post.num_comments,
                "created_utc": str(datetime.fromtimestamp(int(post.created_utc)).strftime('%Y-%m-%d %H:%M:%S')).strip(),
                "flair": post.link_flair_text,
                "is_original_content": post.is_original_content,
                "is_self": post.is_self,
                "over_18": post.over_18,
                "stickied": post.stickied,
                "permalink": "https://reddit.com" + post.permalink,
                })            
            except Exception as e:
                logging.error(f'get_reddit_list: Error getting post.',e)
                print(e)
    
    if len(postlist) > 10:
        logging.info(f'get_reddit_list: Successfully retrieved reddit list for {sub} for period {period}')
    else:
        logging.error(f'get_reddit_list: Failure to retrieve reddit list for {sub} for period {period}')
    return postlist




# Download video posts

def download_video(url,path):
    reddit = Downloader(max_q=True)
    reddit.url = url
    reddit.path = path
    reddit.download()


###########################
##### MAIN FUNCTIONS ######
###########################

# Download by period of time = Input subreddit and period of time to create working directory and collect mp4 files

def main_dl_period(sub, period):
    try:
        global working_dir

        # Get list of posts for download
        dlList = get_dl_list_period(f'{sub}', f'{period}')
        today = datetime.today().strftime('%m-%d-%Y')

        # Run peertube auth to get token for this session
        peertube_auth()
        
        logging.info(f'main_dl_period: Beginning main_dl_period creating playlist for {sub} and period {period}.')
        
        # Create playlist for session
        playlist_id = create_playlist(f'{sub} top of the {period} - Generated {today}', sub)

        # Download each video post
        logging.info(f'main_dl_period: Downloading video posts for for {sub} and period {period}.')
        
        if len(dlList) > 0:
            for post in dlList:
                logging.debug(post[1])
                print(f"Downloading {dlList.__len__} posts.")
                sani_title = cleanString(post[0])
                id = post[2]
                url = str(post[1])

                # Download video and store in working directory
                try:
                    download_video(url, working_dir)
                except Exception as e:
                    print(f"Error downloading video: {e}")
                    logging.error(f"main_dl_period: Error downloading video: {e}")
                    continue
                time.sleep(0.500)

                working_file = glob.glob(f"{working_dir}/*.mp4")[0]

                # Generate video hash
                try:
                    v_hash = VideoHash(path=working_file)
                    hash_value = v_hash.hash
                    logging.debug(f'get video hash: Video hash is {hash_value}')
                except Exception as e:
                    print(f"Error generating video hash: {e}")
                    logging.error(f"main_dl_period: Error generating video hash: {e}")
                    continue


                # Upload video to peertube
                try:
                    vid_uuid = upload_video(sub, sani_title, working_file)
                except Exception as e:
                    print(f"Error uploading video: {e}")
                    logging.error(f"main_dl_period: Error uploading video: {e}")
                    continue

                # Add video hash and path to database
                try:
                    update_item_db(sub, hash_value, id, vid_uuid)
                except Exception as e:
                    print(f"Error updating database: {e}")
                    logging.error("main_dl_period: Error updating database: {e}")
                    continue

                # Add video to same playlist
                try:
                    add_video_playlist(vid_uuid, playlist_id)
                except Exception as e:
                    print(f"Error adding video to playlist: {e}")
                    logging.error(f'main_dl_period: Error adding video to playlist: {e}')
                    continue
                
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


def cleanup_workingdir():
    working_folder_list = glob.glob(f"{working_dir}/*.mp4")
    for i in working_folder_list:
        os.remove(i)
    return print("Cleaned up working directory")


def update_DB():
    global sublist
    for sub in sublist:
        #drop_table(f"subreddit_{sub}")
        #create_sub_table(f"{sub}")
        #print(f"Table {sub} created moving on to download posts")
        for period in ["week","month","year","all"]:
            print(f'Gathering top posts from {sub} for the {period}...')
            postlist = get_reddit_list(sub,period)
            print(f'Found {len(postlist)} posts. Now storing them...')
            store_reddit_posts(sub,postlist)
            print(f'Finished storing top of {sub} for the {period}')


def grab_dat(period):
    global sublist
    for sub in sublist:
        main_dl_period(sub, period)
        print(f'Completed downloading {sub}')


# def main():
#     global sublist
#     choicelist = {}
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
#             print("1. Day")
#             print('2. Week')
#             print('3. Month')
#             print('4. Year')
            
#             choice_period = input("Enter your choice: ")
            
#             if choice_period == '1':
#                 print("You've chosen day.")

#             if choice_period == '2':
#                 print("You've chosen week.")

#             if choice_period == '3':
#                 print("You've chosen month.")

#             if choice_period == '4':
#                 print("You've chosen year.")


#             choicelist = {
#                     'num': 'sub' 
#                 }
#             print(f"Download the top of {choice_period} for which subreddit? \nSelect an option below: ")

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
#                 main_dl_period(choice_sub, choice_period)
#             except KeyboardInterrupt:
#                 print("KeyboardInterrupted!")
#                 break

#         elif choice == "2":

#             print("You selected option 2. \nNow gathering and downloading posts...")

#         elif choice == "3":

#             print("You selected option 3. \nBeginning db update now...")

#         elif choice == "4":

#             print("Exiting...")
#             break

#         else:
#             print("Invalid choice. Please try again.")

# if __name__ == "__main__":
#     main()

#### OLD ####

# Download by sub and number = Input subreddit and number of top posts to collect to create working directory and collect mp4 files

# def main_num(sub,num):
#     #sub = 'funny'
#     #period = 'day'
#     global working_dir
#     today = datetime.today().strftime('%m-%d-%Y')
#     playlist = get_video_posts_num(f'{sub}',num)
#     # uuid_value = str(uuid.uuid4())
#     # parent_dir = working_dir
#     path = f'{storage_dir}/{sub}_Top_{num}_{today}/'
#     os.mkdir(path)

#     for post in playlist:
#         print (post["permalink"])
#         sani_title = cleanString(post["title"])
#         download_video(post["permalink"],working_dir)
#         time.sleep(0.500)
#         old_filename = glob.glob(f"{working_dir}/*.mp4")
#         new_filename = f'{storage_dir}/{sub}_Top_{num}_{today}/{sani_title}.mp4'
#         try:
#             Path(old_filename[0]).rename(new_filename)
#         except:
#             continue
#         print(f'Moving {old_filename[0]} to {new_filename}')

# def get_video_posts_num(sub,num):
#     headers = {'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.2171.95 Safari/537.36'}
#     video_urllist = []
#     #sub = 'funny'
#     #num = 500
#     for post in get_reddit_list_number(f'{sub}',num):
#         if post["permalink"]:
#             if post["permalink"][len(post["permalink"])-1] == '/':
#                 json_url = post["permalink"][:len(post["permalink"])-1]+'.json'
#             else:
#                 json_url = post["permalink"] + '.json'
#             json_response = requests.get(json_url, 
#                             headers= headers)
#             if json_response.json()[0]['data']['children'][0]['data']['is_video'] == True and json_response.json()[0]['data']['children'][0]['data']['over_18'] == False:
#                 json_urllist = {
#                     "id": json_response.json()[0]['data']['children'][0]['data']['id'],
#                     "permalink": post["permalink"],
#                     "title": json_response.json()[0]['data']['children'][0]['data']['title']
#                     }
#                 video_urllist.append(json_urllist)
#             elif json_response.status_code != 200:
#                 print("Error Detected, check the URL!!!")
#     return video_urllist  