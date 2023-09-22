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
