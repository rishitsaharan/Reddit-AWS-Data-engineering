import praw
from praw import Reddit
from utils.constants import SECRET, CLIENT_ID, POST_FIELDS
import sys
import pandas as pd
import numpy as np

def connect_reddit() -> Reddit:
    try:
        reddit = praw.Reddit(
            client_id=CLIENT_ID,
            client_secret=SECRET,
            user_agent='Airscholar Agent'
        )
        print("Connected to Reddit")
        return reddit
    except Exception as e:
        print(e)
        sys.exit(1)
        
def extract_posts(reddit : Reddit, subreddit : str, time_filter = 'day', limit = None):
    subreddit = reddit.subreddit(subreddit)
    posts = subreddit.top(time_filter=time_filter, limit=limit)
    
    post_lists=[]
    for post in posts:
        post_dict = vars(post)
        post = {key : post_dict[key] for key in POST_FIELDS}
        post_lists.append(post)

    return post_lists

def transform_data(posts: list):
    post_df = pd.DataFrame(posts)
    post_df['created_utc'] = pd.to_datetime(post_df['created_utc'], unit='s')
    post_df['over_18'] = np.where((post_df['over_18'] == True), True, False)
    post_df['author'] = post_df['author'].astype(str)
    edited_mode = post_df['edited'].mode()
    post_df['edited'] = np.where(post_df['edited'].isin([True, False]),
                                 post_df['edited'], edited_mode).astype(bool)
    post_df['num_comments'] = post_df['num_comments'].astype(int)
    post_df['score'] = post_df['score'].astype(int)
    post_df['title'] = post_df['title'].astype(str)
    
    return post_df
    
def load_data_to_csv(df, file_path : str):
    df.to_csv(f'{file_path}.csv')