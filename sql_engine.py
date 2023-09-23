import mysql.connector
from mongodb_engine import extract_from_mongodb
import pandas as pd 

"""
connection_string='youtubeproject.cwoakibr9oeh.ap-south-1.rds.amazonaws.com'
user_name='ThineshKumar'
password='ThiKum10203040!'

connection_object = mysql.connector.connect(
host=connection_string,
user=user_name,
password=password)

cursor_object=connection_object.cursor()
cursor_object.execute("use youtube_project")
connection_object.commit()
"""

# button to click on to load from warehouse to sql through streamlit
def transfer_to_sql(channel_name,cursor_object,client,connection_object):
     client=client
     channel_name=channel_name
     channel_name=channel_name.split(' ')
     channel_name='_'.join(channel_name)
     a_,b_,c_=extract_from_mongodb(channel_name,client)
     #channel_name=a_['channel_name']

     #a- channel_info
     #b- video_info
     #c-comments_info

     # extract channel_info
     channel_id = a_[0].get("Channel_id")
     channel_Name = a_[0].get("Channel_Name")
     country = a_[0].get("country")
     playlist_id = a_[0].get("playlsit_id")
     views = a_[0].get("views")
     subscribers = a_[0].get("subcribers")  
     videos = a_[0].get("videos")

     sql = "INSERT INTO channel_info (channel_id, channel_Name, country, playlsit_id, views, subcribers, videos) VALUES (%s, %s, %s, %s, %s, %s, %s)"

     values = (channel_id, channel_Name, country, playlist_id, views, subscribers, videos)

     cursor_object.execute(sql, values)
     connection_object.commit()

     # video_info extraction
     for b in b_:
          video_id=b.get('video_id')
          video_Title=b.get('channelTitle')
          video_Title=b.get('title')
          description_of_video=b.get('description')
          publishedAt=b.get('publishedAt')
          viewCount=b.get('viewCount')
          likeCount=b.get('likeCount')
          commentCount=b.get('commentCount')
          duration=b.get('duration')
          sql = "INSERT INTO video_info (video_id,channel_Title,video_Title,description_of_video,publishedAt,viewCount,likeCount,commentCount,duration) VALUES (%s, %s, %s, %s,%s,%s,%s,%s)"
          values = (video_id,channel_Title,video_Title,description_of_video,publishedAt,viewCount,likeCount,commentCount,duration)
          cursor_object.execute(sql, values)
          connection_object.commit()
     

    #comments extraction

     for c in c_:    
          video_id = c.get("video_id")
          comments = c.get("Comments")
          comment_likes = c.get("comment_likes",0).get("$numberInt", 0)
          reply_count = c.get("reply_count",0).get("$numberInt", 0)
          sql = "INSERT INTO comments_and_replies (video_id, Comments, comment_likes, reply_count) VALUES (%s, %s, %s, %s)"
          values = (video_id, comments, comment_likes, reply_count)
          cursor_object.execute(sql, values)
          connection_object.commit()
     return f"Succesfully loaded the {channel_Name} Data from the Data Lake to Data Warehouse"

# 2part
def list_of_channels(cursor_object):
     cursor_object=cursor_object
     cursor_object.execute("select channel_name from channel_info")
     list_of_channels=cursor_object.fetchall()
     df=pd.DataFrame(list_of_channels,columns=['List of channels',] )
     return df


"""
query Output need to displayed as table in Streamlit Application:

How many comments were made on each video, and what are their
 corresponding video names?
Which videos have the highest number of likes, and what are their 
corresponding channel names?
What is the total number of likes and dislikes for each video, and what are 
their corresponding video names?
What is the total number of views for each channel, and what are their 
corresponding channel names?
What are the names of all the channels that have published videos in the year
 2022?
What is the average duration of all videos in each channel, and what are their 
corresponding channel names?
Which videos have the highest number of comments, and what are their 
corresponding channel names?



"""







#3rd part
def q1(cursor_object):
     # What are the names of all the videos and their corresponding channels?
     #must use join
     cursor_object.execute('select video_Title, from video_info')
     video_titles=cursor_object.fetchall()
     df=pd.DataFrame(video_titles)
     return df
          
def q2(cursor_object):
     # Which channels have the most number of videos, and how many videos do they have?
     cursor_object.execute('select Channel_Name, videos from channel_info where max(videos)')
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df

def q3(cursor_object):
     
     #What are the top 10 most viewed videos and their respective channels?
     #join
     cursor_object.execute('select video_id,channel_Title,viewCount,chanel_Name from video_info order by viewCount limit 10')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df
     


def q4(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df



def q5(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df



def q6(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df


def q7(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df


def q8(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df


def q9(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df


def q10(cursor_object):
     cursor_object.execute('')
     cursor_object.fetchall()
     x=cursor_object.fetchall()
     df=pd.DataFrame(x)
     return df