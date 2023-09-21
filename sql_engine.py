import mysql.connector
from youtube_engine import get_channel_info
from mongodb_engine import *
import pandas as pd 

connection_string='youtubeproject.cwoakibr9oeh.ap-south-1.rds.amazonaws.com'
user_name=ThineshKumar
password='ThiKum10203040!'

connection_object = mysql.connector.connect(
  host=connection_string,
  user=user_name,
  password=password)

cursor_object=connection_object.curosr()

#1st part channel_name in sql 
#user input from streamlit 
def channel_checker(channel_id):
     
     channel_data=get_channel_info(youtube_object=youtube_object,channel_id=channel_id)
     channel_name=channel_data[0]['Channel_Name']

     list_of_channels

     cursor_object.execute("use youtube_project")
     cursor_object.commit()
     cursor_object.execute('select Channel_Name from channel_info')
     databases=cursor_object.fetchall()
     
     if channel_name in  databases:
          #streamlit output
          print('The channel is present in the data warehouse!')

     else:
          #streamlit output
          print('click to load to data lake ')
          load()
# button to click on to load from warehouse to sql through streamlit
def transfer_to_sql():
     a_,b_,c_=extract_from_mongodb()
     channel_name=a['channel_name']

     #a- channel_info
     #b- video_info
     #c-comments_info

     # extract channel_info
     channel_id = a_.get("Channel_id")
     channel_Name = a_.get("Channel_Name")
     country = a_.get("country")
     playlist_id = a_.get("playlsit_id")
     views = a_.get("views")
     subscribers = a_.get("subcribers")  
     videos = a_.get("videos")

     sql = "INSERT INTO channel_info (channel_id, channel_Name, country, playlsit_id, views, subcribers, videos) VALUES (%s, %s, %s, %s, %s, %s, %s)"

     values = (channel_id, channel_Name, country, playlist_id, views, subscribers, videos)

     cursor_object.execute(sql, values)

     # video_info extraction
     for b in b_:
          video_id=b.get('video_id')
          channelTitle=b.get('channelTitle')
          description_of_video=b.get('description')
          publishedAt=b.get('publishedAt')
          viewCount=b.get('viewCount')
          likeCount=b.get('likeCount')
          commentCount=b.get('commentCount')
          duration=b.get('duration')
          sql = "INSERT INTO video_info (video_id,channelTitle,description_of_video,publishedAt,viewCount,likeCount,commentCount,duration) VALUES (%s, %s, %s, %s,%s,%s,%s,%s)"
          values = (video_id,channelTitle,description_of_video,publishedAt,viewCount,likeCount,commentCount,duration)
          cursor_object.execute(sql, values)
          connection_object.commit()
     

    #comments extraction

     for c in c_:    
          video_id = c.get("video_id")
          comments = c.get("Comments")
          comment_likes = c.get("comment_likes", {}).get("$numberInt", 0)
          reply_count = c.get("reply_count", {}).get("$numberInt", 0)
          sql = "INSERT INTO comments_and_replies (video_id, Comments, comment_likes, reply_count) VALUES (%s, %s, %s, %s)"
          values = (video_id, comments, comment_likes, reply_count)
          cursor_object.execute(sql, values)
          connection_object.commit()


# 2part
def list_of_channels():
     cursor_object.execute("select Channel_name from Channel_data")
     list_of_channels=cursor_object.fetchall()
     channel_list=list_of_channels()
     df=pd.Dataframe(channel_list)
     return df






"""  

#3rd part
def question1():
     pass
def q2():
     pass

"""
