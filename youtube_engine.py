from apiclient.discovery import build
from googleapiclient.errors import HttpError


#Generated API key from Google Api Services
API_KEY = "AIzaSyDeazLgd1T6hUwdvraWC6BKv5L1bpB_pgU"

YOUTUBE_API_SERVICE_NAME = "youtube"
YOUTUBE_API_VERSION = "v3"


# creating Youtube Resource Object
youtube_object = build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
                       developerKey = API_KEY)

#obtain channel_id from the user via user's input
channel_id="UCjWY5hREA6FFYrthD0rZNIw"

def get_channel_info(youtube_object,channel_id):
    try:
        Channel_data=[]
        request = youtube_object.channels().list(
            part="snippet,contentDetails,statistics",
            id=channel_id
            )
    
        response = request.execute()
    
        for item in response["items"]:
            data={
                'Channel_Name':item['snippet']['title'],
                'Channel_id':item['id'],
                'country':item['snippet']['country'],
                'playlsit_id':item['contentDetails']['relatedPlaylists']['uploads'],
                'views':item["statistics"]['viewCount'],
                'subcribers':item['statistics']['subscriberCount'],
                'videos':item['statistics']['videoCount']
                }
        Channel_data.append(data)
    
    except HttpError as e:
    # Print the error details
    print(f"HttpError: {e}")
    print(f"Error content: {e.content}")
    print(f"Error details: {e.resp}")
    
    return Channel_data

 
def get_video_ids(youtube_object, playlist_Id):

# defining a function for extracting all the Video ID from the channel after extracing the playlist ID.
    
    video_ids = []  # Initialize the list to store video IDs
    
    try:
        next_page_token = None
        
        while True:
            request = youtube_object.playlistItems().list(
                part="snippet,contentDetails",
                playlistId=playlist_Id,
                maxResults=50,
                pageToken=next_page_token
            )
            response = request.execute()
            
            for item in response.get('items', []):
                video_id = item['contentDetails']['videoId']
                video_ids.append(video_id)
            
            next_page_token = response.get('nextPageToken')
            
            if not next_page_token:
                break  # No more pages to retrieve
    
    except HttpError as e:
        # Handle HTTP errors if necessary
        print(f"HttpError: {e}")
        print(f"Error content: {e.content}")
        print(f"Error details: {e.resp}")
    
    return video_ids  # Return the collected video IDs
