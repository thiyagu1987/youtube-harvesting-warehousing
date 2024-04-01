from googleapiclient.discovery import build
import streamlit as st
import pandas as pd 
import pymongo
import psycopg2


def api_connect (): 
    api_id = 'AIzaSyC-ccfESd6QYs6gVNpr9GNwlNHLEaDvKlQ'
    api_service_name = "youtube"
    api_version ="v3"
    
    youtube = build(api_service_name,api_version, developerKey= api_id)
    return youtube
youtube = api_connect()


def get_channel_info(Channel_id):
    
    request = youtube.channels().list (part='snippet,contentDetails,statistics',
                                    id=Channel_id
)
    response = request.execute()
    
    for i in response ['items']:
        data_info=dict(channel_name = i['snippet']['title'],
                    channel_id = i['id'],
                    channel_Des = i['snippet']['description'],
                    published_date = i['snippet']['publishedAt'],
                    playlist_ID= i['contentDetails']['relatedPlaylists']["uploads"],
                    stats_views = i['statistics']['viewCount'],
                    stats_sub_count  = i['statistics']['subscriberCount'],
                    stats_video_count = i['statistics']['videoCount']
                    )
    return data_info

def get_playlist_detail(Channel_id):
    playlist = []
    next_page_token = None
    while True : 
        request  = youtube.playlists().list(part = 'snippet, contentDetails', channelId = Channel_id, maxResults = 50, pageToken = next_page_token)
        response =request.execute()


        for item in response['items']:
            playlist_data = dict (playlist_id = item ['id'],
                                channel_id  = item ['snippet'] ['channelId'],
                                channel_title  = item ['snippet'] ['title'],
                                channel_Name  = item ['snippet'] ['channelTitle'],
                                Published_date = item ['snippet'] ['publishedAt'],
                                video_count = item ['contentDetails']['itemCount']
                                )
            playlist.append(playlist_data)
        next_page_token = response.get('nextPageToken')
        if next_page_token is None:    
            break
    return(playlist)    
    
    
    # get video_ids 
def get_playlist_ids (Channel_id): 
        Video_ids = []
        request = youtube.channels().list(part ='contentDetails',id = Channel_id)
        response = request.execute()
                
        playlist_id = response ['items'][0]['contentDetails']['relatedPlaylists']["uploads"]
        next_page_token = None
                
        while True:  
                request = youtube.playlistItems().list(part='snippet, contentDetails', 
                                                        playlistId= playlist_id, 
                                                        maxResults=50, 
                                                        pageToken=next_page_token)
                response = request.execute()
                
                for i in range(len(response['items'])):
                        Video_ids.append(response['items'][i]['snippet']['resourceId']['videoId'])
                next_page_token = response.get('nextPageToken')  
                        
                if next_page_token is None:  
                        break
        return Video_ids

# get video info 
def get_video_info(vid_ids):
    
    Video_data = []
    try:
        for video_ids in vid_ids: 
            request = youtube.videos().list(
                                            part='snippet, contentDetails, statistics', 
                                            id= video_ids)
            response = request.execute()
            
            for item in response['items']:
                data_info_2 = dict( 
                                    Channel_name = item ['snippet']['channelTitle'],
                                    Channel_id = item ['snippet']['channelId'],
                                    Video_id = item ['id'],
                                    Title  = item ['snippet']['title'],
                                    Tag = item['snippet'].get('tags'),
                                    Thumbnail = item ['snippet']['thumbnails']['default']['url'],
                                    Description =item['snippet']['description'],
                                    Published_date= item ['snippet']['publishedAt'],
                                    Duration = item ['contentDetails']['duration'],
                                    Viewers = item ['statistics']['viewCount'],
                                    likes = item ['statistics']['likeCount'],
                                    Comments =  item ['statistics']['commentCount'],
                                    Favorite = item['statistics']['favoriteCount'],
                                    Definition = item ['contentDetails']['definition'],
                                    Caption = item ['contentDetails']['caption']
                                    )
            Video_data.append(data_info_2)
    
    except:
        pass 
    return Video_data

# get comment info
def get_comment_details(vid_ids):
    comments  = []
    try: 
        for video_item in vid_ids: 
            request = youtube.commentThreads().list(part= 'snippet', videoId = video_item, maxResults=100)
            response = request.execute()

            for item in response ['items']:
                comment_details = dict(
                                    Comment = item ['snippet']['topLevelComment']['id'],
                                    Video_Id = item['snippet']['topLevelComment']['snippet']['videoId'],
                                    userComments =  item['snippet']['topLevelComment']['snippet']['textDisplay'],
                                    Author =  item['snippet']['topLevelComment']['snippet']['authorDisplayName'],
                                    user_rating =  item['snippet']['topLevelComment']['snippet']['viewerRating'],
                                    user_likes = item['snippet']['topLevelComment']['snippet']['likeCount'],
                                    commented_on = item['snippet']['topLevelComment']['snippet']['publishedAt'])
                comments.append(comment_details)
            
    except: 
        pass  
    return comments  

    
    # creating collections in mongodb 
client = pymongo.MongoClient("mongodb://localhost:27017")
db = client["Youtube_data_harvesting"]

def channel_details (Channel_id):
    ch_details = get_channel_info(Channel_id)
    Pl_details = get_playlist_detail(Channel_id)
    vi_ids = get_playlist_ids (Channel_id)
    vi_details = get_video_info(vi_ids)
    Comments_details = get_comment_details(vi_ids)


    # creating collections in mongodb 

    coll1 = db ['channel_details']
    coll1.insert_one({'channel_info' : ch_details, 'playlist_info': Pl_details,'video_ids':vi_ids, 
                    'video_info' : vi_details,'comment_info': Comments_details})

# Insert channel data in postgres sql from mongodb 

def channels_details():
# Establish connection to PostgreSQL server
    conn = psycopg2.connect(
                host="localhost",
                user="postgres",
                password="dreAmcatcher",
                database="youtube_dataHarvesting",
                port="5432")
    cursor = conn.cursor()
    
    drop_channels  = ''' drop table if exists channels'''
    cursor.execute(drop_channels)
    conn.commit()
        
    create_query  =  '''create table channels(
                                channel_name varchar(100),
                                channel_id varchar(80) primary key,
                                channel_Des text,
                                published_date timestamp,
                                playlist_ID varchar(80) ,
                                stats_views bigint, 
                                stats_sub_count bigint,
                                stats_video_count int
                                )'''
    cursor.execute(create_query)
    conn.commit()
    
    channel_list = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details']
    
    for channel_data in coll1.find({},{'_id':0,"channel_info":1}):
        channel_list.append(channel_data['channel_info'])
        df= pd.DataFrame(channel_list)
        for i in df.to_records().tolist():
            print(i[1:])

    # iterating in DF  and inserting the values  
    for index, row in df.iterrows():
        insert_query = '''
            INSERT INTO channels (
                channel_name,
                channel_id,
                channel_Des,
                published_date,
                playlist_ID,
                stats_views,
                stats_sub_count,
                stats_video_count
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                '''
        values = (
            row['channel_name'],
            row['channel_id'],
            row['channel_Des'],
            row['published_date'],
            row['playlist_ID'],
            row['stats_views'],
            row['stats_sub_count'],
            row['stats_video_count']
    )
        cursor.execute(insert_query, values)
        conn.commit() 
    
    
# Playlist details from mongodb toPostgres 

def playlist_table():
    conn = psycopg2.connect(
                    host="localhost",
                    user="postgres",
                    password="dreAmcatcher",
                    database="youtube_dataHarvesting",
                    port="5432")
    cursor = conn.cursor()

    playlist_drop = '''drop table if exists playlist'''
    cursor.execute(playlist_drop)
    conn.commit()

    create_query_1  =  '''create table playlist(
                                    playlist_id varchar(100) primary key,
                                    channel_id varchar(100),
                                    channel_title varchar(200),
                                    channel_Name varchar(200),
                                    Published_date timestamp,
                                    video_count int
                                    )'''
    cursor.execute(create_query_1)
    conn.commit()

    playlists = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details' ] 

    for play_data in coll1.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(play_data["playlist_info"])):
            playlists.append(play_data["playlist_info"][i])
            df_1= pd.DataFrame(playlists)
            
    for index, row in df_1.iterrows():
        insert_query = '''
            INSERT INTO playlist (
            playlist_id,
            channel_id,
            channel_title,
            channel_Name,
            Published_date,
            video_count
            ) VALUES (%s, %s, %s, %s, %s, %s)'''
            
        values = (
            row['playlist_id'],
            row['channel_id'],
            row['channel_title'],
            row['channel_Name'],
            row['Published_date'],
            row['video_count']
            )
        cursor.execute(insert_query, values)
        conn.commit()
        
# establishing video ids from mongo to postgres 

def video_details():
    conn = psycopg2.connect(
                        host="localhost",
                        user="postgres",
                        password="dreAmcatcher",
                        database="youtube_dataHarvesting",
                        port="5432")
    cursor = conn.cursor()

    drop_table = '''drop table if exists video_details'''
    cursor.execute(drop_table)
    conn.commit

    insert_table = '''create table video_details 
                    (
                    Channel_name varchar(200),
                    Channel_id varchar (100),
                    Video_id varchar (100) primary key,
                    Title varchar (100),
                    Tag text,
                    Thumbnail varchar (100),
                    Description text,
                    Published_date timestamp,
                    Duration interval,
                    Viewers bigint,
                    likes bigint,
                    Comments int, 
                    Favorite varchar (10),
                    Definition varchar (100),
                    Caption varchar (100) 
                    )'''
    cursor.execute(insert_table)
    conn.commit()

    video_list = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details' ] 

    for vid_details in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_details["video_info"])):
            video_list.append(vid_details["video_info"][i])
            df_2 = pd.DataFrame(video_list)
            
    for index, row in df_2.iterrows():
            insert_query = '''
                INSERT INTO video_details (
                    Channel_name,
                    Channel_id,
                    Video_id,
                    Title,
                Tag,
                Thumbnail,
                Description,
                Published_date,
                Duration,
                Viewers,
                likes,
                Comments, 
                Favorite,
                Definition,
                Caption  
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''
            
            values = (
                row['Channel_name'],
                row['Channel_id'],
                row['Video_id'],
                row['Title'],
                row['Tag'],
                row['Thumbnail'],
                row['Description'],
                row['Published_date'],
                row['Duration'],
                row['Viewers'],
                row['likes'],
                row['Comments'],
                row['Favorite'],
                row['Definition'],
                row['Caption']
            )
            cursor.execute(insert_query, values)
            conn.commit()

# comment details from mongodb to postgres 

def comment_details():
    conn = psycopg2.connect(host="localhost",
                            user="postgres",
                            password="dreAmcatcher",
                            database="youtube_dataHarvesting",
                            port="5432")

    cursor = conn.cursor()

    drop_table  = ''' drop table if exists comments'''
    cursor.execute(drop_table)
    conn.commit()

    insert_query =  ''' create table comments (
                                                Comment Varchar(100) primary key,
                                                Video_Id varchar(100),
                                                userComments text,
                                                Author varchar(100),
                                                user_rating varchar(100),
                                                user_likes int,
                                                commented_on timestamp
                                                                    )'''
    cursor.execute(insert_query)
    conn.commit()


    comment_det = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details']

    for com_details in coll1.find({},{'comment_info':1}):
        for i in range(len(com_details['comment_info'])):
            comment_det.append(com_details['comment_info'][i])
            df_4 = pd.DataFrame(comment_det)
            
    for index, row in df_4.iterrows():
            insert_query = '''
                INSERT INTO comments (
                                        Comment,
                                        Video_Id,
                                        userComments,
                                        Author,
                                        user_rating,
                                        user_likes,
                                        commented_on
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)'''
                
            values = (
                row['Comment'],
                row['Video_Id'],
                row['userComments'],
                row['Author'],
                row['user_rating'],
                row['user_likes'],
                row['commented_on']
                )
            cursor.execute(insert_query, values)
            conn.commit()


def tables():
    channels_details()
    playlist_table()
    video_details()
    comment_details()
    
    return "table created Successfully"



def view_channels():

    channel_list = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details']
    for channel_data in coll1.find({},{'_id':0,"channel_info":1}):
        channel_list.append(channel_data['channel_info'])
    df = st.dataframe(channel_list)
    
    return (df)

def view_playlist_info():

    playlists = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details' ] 

    for play_data in coll1.find({},{"_id":0,"playlist_info":1}):
        for i in range(len(play_data["playlist_info"])):
            playlists.append(play_data["playlist_info"][i])
    df_1 = st.dataframe(playlists)
    
    return (df_1)

def view_video_info():  

    video_list = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details' ] 

    for vid_details in coll1.find({},{"_id":0,"video_info":1}):
        for i in range(len(vid_details["video_info"])):
            video_list.append(vid_details["video_info"][i])
    df_2 = st.dataframe(video_list)
    
    return (df_2)   

def comments_info ():
    
    comment_det = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details']

    for com_details in coll1.find({},{'comment_info':1}):
        for i in range(len(com_details['comment_info'])):
            comment_det.append(com_details['comment_info'][i])
    df_4 = st.dataframe(comment_det)
    
    return (df_4)

# streamlit establishment 

with st.sidebar: 
    st.title (":red[Youtube DATA SCARPING AND WAREHOUSE]")
    st.header(" project projection")
    st.caption("python codes")
    st.caption("data migration")
    st.caption("MongoDB data warehouse")
    st.caption("postgres data warehouse & tabular")
    st.caption("streamlit Data Visualization")
    
channel_ids = st.text_input("Enter channel_id")

if st.button ("Channel_id storage"):
    
    Channel_id = []
    db = client["Youtube_data_harvesting"]
    coll1=db ['channel_details']
    
    for chl_data in coll1.find({},{'_id':0,"channel_info":1}):
        Channel_id.append(chl_data['channel_info']['channel_id'])
        
    if channel_ids in Channel_id:
        st.success("channel already exists")
    
    else:
        insert = channel_details(channel_ids)
        st.success("channel successfully found")
    
    
    
if st.button("Migration to SQL"):
    Show_table = tables()
    st.success("Data migrated successfully")

Tables_info = st.radio("Select to view",("Channel_info","Playlist_info","Video_info","User_comments"))

if Tables_info=="Channel_info":
    view_channels()

if Tables_info=="Playlist_info":
    view_playlist_info()

if Tables_info=="Video_info":
    view_video_info()

if Tables_info=="User_comments":
    comments_info ()


#connect with postgresql 

conn = psycopg2.connect(host="localhost",
                        user="postgres",
                        password="dreAmcatcher",
                        database="youtube_dataHarvesting",
                        port="5432")
cursor = conn.cursor()


Query = st.selectbox("choose your the query",(
                                        "1. All Videos and channels",
                                        "2. channels with most number of videos",
                                        "3. Top 10 viewed videos",
                                        "4. Comments in each videos",
                                        "5. Videos with most likes",
                                        "6. All video likes",
                                        "7. Total views",
                                        "8. Channels Published 2022",
                                        "9. Avg duration of all videos",
                                        "10. videos with Highest comments"
                                        ))
if Query == "1. All Videos and channels" :
        q1 = ''' select Title, Channel_name from video_details'''
        cursor.execute(q1)
        conn.commit()
        data = cursor.fetchall()
        df=pd.DataFrame(data, columns=['TitleName','ChannelName'])
        st.write(df)
        
elif Query == "2. channels with most number of videos" :
        q2 = ''' select channel_name, stats_video_count from channels
                order by stats_video_count desc '''
        cursor.execute(q2)
        conn.commit()
        data_1 = cursor.fetchall()
        df_1=pd.DataFrame(data_1, columns=['ChannelName','Total videos'])
        st.write(df_1)
        
elif Query == "3. Top 10 viewed videos" :
        q3 = ''' select Channel_name, Title, Viewers from video_details
                where Viewers is not null order by Viewers desc limit 10'''
        cursor.execute(q3)
        conn.commit()
        data_2 = cursor.fetchall()
        df_2=pd.DataFrame(data_2, columns=['ChannelName','TitleName','TopViewers'])
        st.write(df_2)
        
elif Query == "4. Comments in each videos" :
        q4 = ''' select Comment,userComments, Author from comments'''
        cursor.execute(q4)
        conn.commit()
        data_3 = cursor.fetchall()
        df_3=pd.DataFrame(data_3, columns=['CommentId','UserComments','Author'])
        st.write(df_3)

elif Query == "5. Videos with most likes" :
        q5 = ''' select Channel_name, Title, likes from video_details 
                where likes is not null order by likes desc limit 10'''
        cursor.execute(q5)
        conn.commit()
        data_4 = cursor.fetchall()
        df_4 =pd.DataFrame(data_4, columns=['ChannelName','TitleName','UserLikes'])
        st.write(df_4)
        
elif Query == "6. All video likes":
        q6 = ''' select Channel_name, Title, likes from video_details
                order by likes desc'''
        cursor.execute(q6)
        conn.commit()
        data_5 = cursor.fetchall()
        df_5 =pd.DataFrame(data_5, columns=['ChannelName','TitleName','UserLikes'])
        st.write(df_5)


elif Query == "7. Total views":
        q7 = ''' select Channel_name, Viewers from video_details
                order by Viewers desc'''
        cursor.execute(q7)
        conn.commit()
        data_6 = cursor.fetchall()
        df_6 =pd.DataFrame(data_6, columns=['ChannelName', 'TotalViewers'])
        st.write(df_6)

elif Query == "8. Channels Published 2022":
        q8 = '''select channel_name, published_date from channels
                where published_date between '2021-12-31' AND '2022-12-31' '''
        cursor.execute(q8)
        conn.commit()
        data_7 = cursor.fetchall()
        df_7 =pd.DataFrame(data_7, columns=['ChannelName', 'publishedAT'])
        st.write(df_7)
        
elif Query == "9. Avg duration of all videos":
        q9 = '''select Channel_name, AVG(Duration) as Avg_duration from video_details
                group by Channel_name '''
        cursor.execute(q9)
        conn.commit()
        data_8 = cursor.fetchall()
        df_8 =pd.DataFrame(data_8, columns=['ChannelName','Avg_duration'])
        
        DATA8 = []
        for index,row in df_8.iterrows():
                channel_name = row["ChannelName"]
                avg_duration = row ['Avg_duration']
                avg_duration_str = str(avg_duration)
                DATA8.append (dict(channel_title = channel_name, Average_duration = avg_duration_str))
                df_08 = pd.DataFrame(DATA8)
        st.write(df_08)

elif Query == "10. videos with Highest comments":

    q10 = '''select Title, Channel_name, Comments from video_details 
                where Comments is not null order by Comments desc limit 10 '''
    cursor.execute(q10)
    conn.commit()
    data_9 = cursor.fetchall()
    df_9 =pd.DataFrame(data_9, columns=['TitleName','ChannelName','comments_counts'])
    st.write(df_9)




