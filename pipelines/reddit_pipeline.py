from etls.reddit_etl import connect_reddit, extract_posts, transform_data, load_data_to_csv
from utils.constants import OUTPUT_PATH

def reddit_pipeline(file_name: str, subreddit : str, time_filter = 'day', limit = None):
    reddit = connect_reddit()
    posts = extract_posts(reddit, subreddit, time_filter, limit)
    
    final_data = transform_data(posts)
    
    file_path = f'{OUTPUT_PATH}/{file_name}.csv'
    load_data_to_csv(final_data, file_path)
    
    
    