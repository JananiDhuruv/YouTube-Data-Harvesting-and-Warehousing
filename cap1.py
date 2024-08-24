from googleapiclient.discovery import build
import googleapiclient.discovery
import pymongo
import mysql.connector
import streamlit as st
import pandas as pd
from datetime import datetime
from datetime import datetime, timedelta
from PIL import Image
import matplotlib.pyplot as plt
import plotly.express as px
import altair as alt
import plotly.graph_objs as go

#api key collectionpip install
def apiconnection():
    api_key="AIzaSyBLKcmEggI4GWPweX6Qm6LfvP5_0A4YA7Q"
    api_service_name = "youtube"
    api_version = "v3"
    youtube = googleapiclient.discovery.build(api_service_name, api_version, developerKey=api_key)
    return youtube
youtube=apiconnection()
#channels
def get_channel_details(channel_id):
    request = youtube.channels().list(
        part="snippet,contentDetails,statistics",
        id=channel_id
    )
    response = request.execute()

    for i in response ['items']:
       data=dict(channel_name=i['snippet']['title'],
                 channel_id=i["id"],
                 sub_count=i['statistics']['subscriberCount'],
                 view_count=i['statistics']['viewCount'],
                 total_videos=i['statistics']['videoCount'],
                 channel_description=i["snippet"]["description"],
                 Playlist_Id=i['contentDetails']['relatedPlaylists']['uploads'])
                                
    return data
#getvideodetails----playlist id
def get_video_ids(channel_id):
        video_ids=[]
        response=youtube.channels().list(id=channel_id,
                        part='contentDetails').execute()
        play_listid=response['items'][0]['contentDetails']['relatedPlaylists']['uploads']
        next_pagetoken=None
        while True:
                response11=youtube.playlistItems().list(
                        part="snippet",
                        playlistId=play_listid,
                        maxResults=50,
                        pageToken=next_pagetoken).execute()
                for i in range(len(response11['items'])):
                        video_ids.append(response11['items'][i]['snippet']['resourceId']['videoId'])
                next_pagetoken=response11.get('nextPageToken')

                if next_pagetoken is None:
                        break
        return video_ids
#video details
def get_video_information(video_ids):
        video_data=[]
        for video_id in video_ids:
                request=youtube.videos().list(
                        part="snippet,contentDetails,statistics",
                        id=video_id
                )
                response=request.execute()

                for item in response["items"]:
                        data3=dict(channel_name=item["snippet"]['channelTitle'],
                                        channel_id=item["snippet"]['channelId'],
                                        video_id=item["id"],
                                        Title=item["snippet"]["title"],
                                        Tags=item['snippet'].get('tags'),
                                        Thumbnail=item["snippet"]['thumbnails']["default"]["url"],
                                        Description=item["snippet"].get("description"),
                                        Published_at=item["snippet"]["publishedAt"],
                                        Duration=item["contentDetails"]["duration"],
                                        views=item['statistics'].get('viewCount',0),
                                        Comments=item["statistics"].get('commentCount',0),
                                        Favourite_count=item["statistics"].get('favoriteCount',0),
                                        Like_count=item["statistics"].get("likeCount",0),
                                        Definition=item["contentDetails"].get('definition'),
                                        Caption_info=item["contentDetails"].get("caption"))
                        video_data.append(data3)
        return video_data

#comment details
def get_comment_info(video_ids):
    comment_data=[]
    try:
        for video_id in video_ids:
            request = youtube.commentThreads().list(
                    part="snippet",
                    videoId=video_id,
                    maxResults=50        #limits

                )
            response=request.execute()
            for item in response['items']:
                data4=dict(Comment_Id=item["snippet"]["topLevelComment"]["id"],
                    video_id=item["snippet"]["topLevelComment"]["snippet"]["videoId"],
                    comment_Author=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment_Text=item["snippet"]["topLevelComment"]["snippet"]["textDisplay"],
                    comment_published=item["snippet"]["topLevelComment"]["snippet"]["publishedAt"])
                comment_data.append(data4)
    except:
        pass
    return comment_data

#playlist details
def get_playlist_detail(channel_id):
        next_page=None
        All_data=[]
        while True:
                request=youtube.playlists().list(
                        part="snippet,contentDetails",
                        channelId=channel_id,
                        maxResults=40,
                        pageToken=next_page
                )                
                response=request.execute()

                for item in response["items"]:
                        data4=dict(playlist_id=item["id"],
                                        Title=item["snippet"]['title'],
                                        channel_name=item["snippet"]['channelTitle'],
                                        channel_id=item["snippet"]['channelId'],
                                        video_count=item["contentDetails"]["itemCount"])
                                        
                        All_data.append(data4)
                next_page=response.get("nextPageToken")                               
                if next_page is None:
                        break             
        return All_data

mongo=pymongo.MongoClient("mongodb://Janani08:guvi2024@ac-hrydaoc-shard-00-00.mgloviw.mongodb.net:27017,ac-hrydaoc-shard-00-01.mgloviw.mongodb.net:27017,ac-hrydaoc-shard-00-02.mgloviw.mongodb.net:27017/?ssl=true&replicaSet=atlas-q0bl1i-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
y_data=mongo["y_databasejanani_mdte"]#db creation

#channel details
def channeldetails(channel_id):
    channel_i=get_channel_details(channel_id)
    video_i=get_video_ids(channel_id)
    video_info=get_video_information(video_i)
    comment_info=get_comment_info(video_i)
    playlist_info=get_playlist_detail(channel_id)
    collection=y_data["channel_full_details"]
    collection.insert_one({"channel_related_info":channel_i,"video_related_info":video_info,"comment_relaed_info":comment_info,"playlist_related_info":playlist_info})
    return "Upload successfully"

def channel_table(): #sucesss def channel_table():
    # Connect to MySQL
    mydatabase = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dhuruvanth@29",
        database="guvi",
        port="3306")
    

    mycursor = mydatabase.cursor(buffered=True)
    drop_q = '''DROP TABLE IF EXISTS channels'''
    mycursor.execute( drop_q)
    mydatabase.commit()

    try:
        
        create_query = '''CREATE TABLE IF NOT EXISTS channels(
                            channel_name VARCHAR(95),
                            channel_id VARCHAR(80) PRIMARY KEY,
                            sub_count BIGINT,
                            view_count BIGINT,
                            total_videos INT,
                            channel_description TEXT,
                            Playlist_Id VARCHAR(70)
                            )''' 
        mycursor.execute(create_query)
        
        
    except:
        print("channel already created")
            
        
    ch_list = []
    y_data=mongo["y_databasejanani_mdte"]
    collection=y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "channel_related_info": 1}):
        ch_list.append(i["channel_related_info"])

    df1 = pd.DataFrame(ch_list)


    for index, row in df1.iterrows():
        qu_y = '''INSERT INTO channels(channel_name,channel_id,sub_count,view_count,total_videos,channel_description,Playlist_Id)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)'''
        values = (
                row["channel_name"],
                row["channel_id"],
                row["sub_count"],
                row["view_count"],
                row["total_videos"],
                row["channel_description"],
                row["Playlist_Id"])
            
            
                
        try:
            mycursor.execute(qu_y,values)
            mydatabase.commit()
        except:
            print("channel value inserted")



def video_table():
    mydatabase = mysql.connector.connect(
                host="localhost",
                user="root",
                password="Dhuruvanth@29",
                database="guvi",
                port="3306"
                )
    mycursor = mydatabase.cursor(buffered=True)

    # Drop table if exists
    drop_q = '''DROP TABLE IF EXISTS videos'''
    mycursor.execute(drop_q)
    mydatabase.commit()

    # Create table with increased length for Thumbnail
    create_query = '''CREATE TABLE IF NOT EXISTS videos(
                    channel_name VARCHAR(100),
                    channel_id VARCHAR(100),
                    video_id VARCHAR(100) PRIMARY KEY,
                    Title VARCHAR(280),
                    Tags TEXT,
                    Thumbnail VARCHAR(500), 
                    Description TEXT,
                    Published_at TIMESTAMP,
                    Duration VARCHAR(50),
                    views BIGINT,
                    Comments TEXT,
                    Favourite_count INT,
                    Like_count BIGINT,
                    Definition VARCHAR(10),
                    Caption_info VARCHAR(100)
                    )'''
   
    mycursor.execute(create_query)
    mydatabase.commit()

    vi_list = []
    y_data = mongo["y_databasejanani_mdte"]
    collection = y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "video_related_info": 1}):
        for k in range(len(i["video_related_info"])):
            vi_list.append(i["video_related_info"][k])
    video_df3 = pd.DataFrame(vi_list)

    # Remove exact duplicates in DataFrame
    video_df3.drop_duplicates(subset=['video_id'], keep='first', inplace=True)

    # Insert data into MySQL
    for index, row in video_df3.iterrows():
        # Convert Published_at to the correct format
        published_at = datetime.strptime(row["Published_at"], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        tags = ', '.join(row["Tags"]) if isinstance(row["Tags"], list) else row["Tags"]
        comments = ', '.join(row["Comments"]) if isinstance(row["Comments"], list) else row["Comments"]

        qu_y = '''INSERT INTO videos(
                channel_name,
                channel_id,
                video_id,
                Title,
                Tags,
                Thumbnail,
                Description,
                Published_at,
                Duration,
                views,
                Comments,
                Favourite_count,
                Like_count,
                Definition,
                Caption_info
                ) 
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                Title = VALUES(Title),
                Tags = VALUES(Tags),
                Thumbnail = VALUES(Thumbnail),
                Description = VALUES(Description),
                Published_at = VALUES(Published_at),
                Duration = VALUES(Duration),
                views = VALUES(views),
                Comments = VALUES(Comments),
                Favourite_count = VALUES(Favourite_count),
                Like_count = VALUES(Like_count),
                Definition = VALUES(Definition),
                Caption_info = VALUES(Caption_info)'''

        values = (
                row["channel_name"],
                row["channel_id"],
                row["video_id"],
                row["Title"],
                tags,
                row["Thumbnail"],
                row["Description"],
                published_at,  # Use the converted datetime string
                row["Duration"],
                row["views"],
                comments,
                row["Favourite_count"],
                row["Like_count"],
                row["Definition"],
                row["Caption_info"])

       
        mycursor.execute(qu_y, values)
        mydatabase.commit()
      
def comments_table():
    
    mydatabase = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dhuruvanth@29",
        database="guvi",
        port="3306"
    )

    mycursor = mydatabase.cursor(buffered=True)

    # Drop table if exists
    drop_q = '''DROP TABLE IF EXISTS comments'''
    mycursor.execute(drop_q)
    mydatabase.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS comments(
                    Comment_Id VARCHAR(100) PRIMARY KEY,
                    video_id VARCHAR(60),
                    comment_Author VARCHAR(200), 
                    comment_Text TEXT,
                    comment_published TIMESTAMP
                    )'''
    mycursor.execute(create_query)
    mydatabase.commit()

    # Extract comments data
    co_list = []
    y_data=mongo["y_databasejanani_mdte"]
    collection=y_data["channel_full_details"]
    for i in collection.find({},{"_id": 0, "comment_relaed_info": 1}):
        for k in range(len(i["comment_relaed_info"])):
            co_list.append(i["comment_relaed_info"][k])
    co_df1 = pd.DataFrame(co_list)
    # Insert data into MySQL
    for index, row in co_df1.iterrows():
        comment_published = datetime.strptime(row["comment_published"], '%Y-%m-%dT%H:%M:%SZ').strftime('%Y-%m-%d %H:%M:%S')
        comment_author = row["comment_Author"][:200]
        qu_y = '''INSERT INTO comments(
                Comment_Id, 
                video_id,
                comment_Author,
                comment_Text,
                comment_published
                ) VALUES (%s, %s, %s, %s, %s)'''
            
        values = (
            row["Comment_Id"],
            row["video_id"],
            comment_author,  # Use truncated value
            row["comment_Text"],
            comment_published) 
        
        mycursor.execute(qu_y, values)
        mydatabase.commit()
       

def playlist_table():#sucess
# Connect to MySQL
    mydatabase = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Dhuruvanth@29",
    database="guvi",
    port="3306"
    )

    mycursor = mydatabase.cursor(buffered=True)

    # Drop table if exists
    drop_q = '''DROP TABLE IF EXISTS playlist'''
    mycursor.execute(drop_q)
    mydatabase.commit()

    # Create table
    create_query = '''CREATE TABLE IF NOT EXISTS playlist(
                playlist_id VARCHAR(95) PRIMARY KEY,
                Title VARCHAR(255),
                channel_id VARCHAR(100),
                channel_name VARCHAR(100),
                video_count INT
                )''' 
    mycursor.execute(create_query)
    mydatabase.commit()

    # Extract playlist data
    pl_list = []
    y_data= mongo["y_databasejanani_mdte"]
    collection = y_data["channel_full_details"]
    for doc in collection.find({}, {"_id": 0, "playlist_related_info": 1}):
        for playlist in doc.get("playlist_related_info", []):
            pl_list.append(playlist)
    
    df2 = pd.DataFrame(pl_list)
    # Insert data into MySQL
    for index, row in df2.iterrows():
        qu_y = '''INSERT INTO playlist(
                playlist_id,
                Title,
                channel_id,
                channel_name,
                video_count
                ) 
                VALUES (%s, %s, %s, %s, %s)'''
            
        values = (
                row["playlist_id"],
                row["Title"],
                row["channel_name"],
                row["channel_id"],
                row["video_count"]
                )

        try:
            mycursor.execute(qu_y,values)
            mydatabase.commit()
        except:
            print("Playlist table created successfully")

def All_table():
    channel_table()
    playlist_table()
    video_table()
    comments_table()
    return "All The Tables ARE Creadted Successfully"

def show_channel_table():
    ch_list = []
    y_data = mongo["y_databasejanani_mdte"]
    collection = y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "channel_related_info": 1}):
        ch_list.append(i["channel_related_info"])
    #df1 = pd.DataFrame(ch_list)
    channel_df=st.dataframe(ch_list)
    return channel_df

def show_video_table():
    vi_list = []
    y_data=mongo["y_databasejanani_mdte"]
    collection=y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "video_related_info": 1}):
        for k in range(len(i["video_related_info"])):
            vi_list.append(i["video_related_info"][k])
    vi_df3 = pd.DataFrame(vi_list)
    def safe_convert_to_str(value):
            try:
                if pd.isna(value):
                    return ""
                return str(value)
            except Exception as e:
                return ""

        # Apply conversion to all columns
    for col in vi_df3.columns:
        vi_df3[col] = vi_df3[col].apply(safe_convert_to_str)

    video_df = st.dataframe(vi_df3)  
    return video_df   
   
def show_comment_table():
    comment_list = []
    y_data=mongo["y_databasejanani_mdte"]
    collection=y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "comment_relaed_info": 1}):
        for k in range(len(i["comment_relaed_info"])):
            comment_list.append(i["comment_relaed_info"][k])
        comment_df = st.dataframe(comment_list)
    return comment_df

def show_playlist_table():
    pl_list = []
    y_data = mongo["y_databasejanani_mdte"]
    collection = y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "playlist_related_info": 1}):
        for k in range(len(i["playlist_related_info"])):
            pl_list.append(i["playlist_related_info"][k])
    play_df2= st.dataframe(pl_list)
    return play_df2
def create_marquee(text, color="#007bff"):
    st.markdown(
        f"""
        <style>
        .marquee {{
            width: 100%;
            overflow: hidden;
            white-space: nowrap;
            box-sizing: border-box;
            animation: marquee 10s linear infinite;
            color: {color};
        }}

        @keyframes marquee {{
            0%   {{ text-indent: 100% }}
            100% {{ text-indent: -100% }}
        }}
        </style>
        <div class="marquee">
            {text}
        </div>
        """,
        unsafe_allow_html=True
    )

### streamlit
st.set_page_config(layout="wide")
st.title("YOUTUBE DATA HARVESTING AND WAREHOUSING")
create_marquee("YOUTUBE DATA HARVESTING AND WAREHOUSING -DATA SCIENCE @ GUVI", color="#ff5733")
channel_id=st.text_input("ENTER THE CHANNEL ID")
if st.button("AM READY TO COLLECT CHANNELS "):
    chan_id=[]
    mongo=pymongo.MongoClient("mongodb://Janani08:guvi2024@ac-hrydaoc-shard-00-00.mgloviw.mongodb.net:27017,ac-hrydaoc-shard-00-01.mgloviw.mongodb.net:27017,ac-hrydaoc-shard-00-02.mgloviw.mongodb.net:27017/?ssl=true&replicaSet=atlas-q0bl1i-shard-0&authSource=admin&retryWrites=true&w=majority&appName=Cluster0")
    y_data = mongo["y_databasejanani_mdte"]
    collection = y_data["channel_full_details"]
    for i in collection.find({}, {"_id": 0, "channel_related_info": 1}):
        chan_id.append(i["channel_related_info"]["channel_id"])  
    if channel_id in chan_id:
        st.success("Channel Id Already There....")
    else:
        a=channeldetails(channel_id)
        st.success(a)

if st.button("MIGRATE TO SQL"):
        Table=All_table()
        st.success(Table)
view_table=st.radio("SELECT THE TABLE FOR YOUR VISION",("CHANNELS","PLAYLISTS","VIDEOS","COMMENTS"))

if view_table=="CHANNELS":
    show_channel_table()

elif view_table=="PLAYLISTS":
    show_playlist_table()

elif view_table=="VIDEOS":
    show_video_table()

elif view_table=="COMMENTS":
    show_comment_table()

mydatabase = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Dhuruvanth@29",
        database="guvi",
        port="3306")
    
mycursor = mydatabase.cursor(buffered=True)
my_question=st.selectbox("select your questions",("1.What are the names of all the videos and their corresponding channels?",
        "2.Which channels have the most number of videos, and how many videos do they have?",
        "3.What are the top 10 most viewed videos and their respective channels?",
        "4.How many comments were made on each video, and what are their corresponding video names?",
        "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
        "6.What is the total number of likes for each video, and what are their corresponding video names?",
        "7.What is the total number of views for each channel, and what are their corresponding channel names?",
        "8.What are the names of all the channels that have published videos in the year2022?",
        "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
        "10.Which videos have the highest number of comments, and what are their corresponding channel names?"))

if my_question=="1.What are the names of all the videos and their corresponding channels?":
        q1='''SELECT Title, channel_name FROM videos;'''
        mycursor.execute(q1)
        mydatabase.commit()
        output1 =mycursor.fetchall()
        df1 = pd.DataFrame(output1,columns=["VIDEO TITLE","CHANNEL NAME"])
        st.write(df1)
        


elif my_question=="2.Which channels have the most number of videos, and how many videos do they have?":
        q2='''SELECT channel_name, COUNT(video_id) AS video_count
                FROM videos
                GROUP BY channel_name
                ORDER BY video_count DESC;'''
        mycursor.execute(q2)
        mydatabase.commit()
        output2 =mycursor.fetchall()
        df2 = pd.DataFrame(output2,columns=["CHANNEL NAME","MOST NO OF VIDEOS"])
        st.write(df2)
        st.header("***Plot Visualizations***")
        fig = px.bar(df2, x='MOST NO OF VIDEOS', y='CHANNEL NAME', orientation='h', 
             title='Channels with Most Number of Videos')

        st.plotly_chart(fig)

elif my_question=="3.What are the top 10 most viewed videos and their respective channels?":
        q3='''SELECT channel_name, Title, views
                FROM videos WHERE views is not null
                ORDER BY views DESC
                LIMIT 10;'''
        mycursor.execute(q3)
        mydatabase.commit()
        output3 =mycursor.fetchall()
        df3 = pd.DataFrame(output3,columns=["CHANNEL NAME","VIDEO TITLE","VIEWS"])
        st.write(df3)

elif my_question=="4.How many comments were made on each video, and what are their corresponding video names?":
        q4='''select comments as no_comments,Title as videoTitle from videos where comments is not null;'''
        mycursor.execute(q4)
        mydatabase.commit()
        output4 =mycursor.fetchall()
        df4 = pd.DataFrame(output4,columns=["COMMENTS","VIDEO TITLE"])
        st.write(df4)
        fig = px.bar(df4, x='COMMENTS', y='VIDEO TITLE', orientation='h',
             title='Number of Comments per Video')
        st.header("***Plot Visualizations***")
        fig.update_layout(
            xaxis_title="Number of Comments",
            yaxis_title="Video Title"
        )
        st.plotly_chart(fig)
elif my_question=="5.Which videos have the highest number of likes, and what are their corresponding channel names?":
        q5='''SELECT Title,channel_name,Like_count FROM videos ORDER BY Like_count DESC LIMIT 10;'''
        mycursor.execute(q5)
        mydatabase.commit()
        output5 =mycursor.fetchall()
        df5= pd.DataFrame(output5,columns=["VIDEO TITLE","CHANNEL NAME","LIKE COUNT"])
        st.write(df5)
elif my_question=="6.What is the total number of likes for each video, and what are their corresponding video names?":
        q6='''SELECT Title,Like_count FROM videos ORDER BY Like_count DESC;'''
        mycursor.execute(q6)
        mydatabase.commit()
        output6 =mycursor.fetchall()
        df6= pd.DataFrame(output6,columns=["VIDEO TITLE","LIKE COUNT"])
        st.write(df6)
        st.header("***Plot Visualizations***")
        chart = alt.Chart(df6).mark_bar().encode(
            x='LIKE COUNT:Q',
            y=alt.Y('VIDEO TITLE:N', sort='-x'), 
            tooltip=['VIDEO TITLE', 'LIKE COUNT']
        ).properties(
            title='Total Number of Likes per Video'
        )

        # Display chart using vega-lite renderer in Streamlit
        st.altair_chart(chart, use_container_width=True)
elif my_question=="7.What is the total number of views for each channel, and what are their corresponding channel names?":
        q7='''SELECT channel_name,SUM(views) AS total_views FROM videos GROUP BY 
        channel_name ORDER BY total_views DESC;'''
        mycursor.execute(q7)
        mydatabase.commit()
        output7 =mycursor.fetchall()
        df7= pd.DataFrame(output7,columns=["CHANNEL NAME","TOTAL VIEWS"])
        st.write(df7)
        st.header("***Plot Visualizations***")
        fig = go.Figure(data=[
            go.Bar(
                x=df7['TOTAL VIEWS'],
                y=df7['CHANNEL NAME'],
                orientation='h' 
            )
        ])
        fig.update_layout(
            title='Total Number of Views per Channel',
            xaxis_title="Total Views",
            yaxis_title="Channel Name",
            yaxis={'categoryorder': 'total ascending'}  
        )
        st.plotly_chart(fig)

elif my_question=="8.What are the names of all the channels that have published videos in the year2022?":
        q8='''SELECT Title AS video_title,Published_at AS publishedat,channel_name AS channelname FROM videos WHERE YEAR(Published_at) = 2022;'''
        mycursor.execute(q8)
        mydatabase.commit()
        output8 =mycursor.fetchall()
        df8= pd.DataFrame(output8,columns=["VIDEO TITLE","PUBLISHED DATE","CHANNEL NAME"])
        st.write(df8)
        
elif my_question=="9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
        q9='''SELECT channel_name,AVG(TIME_TO_SEC(TIMEDIFF(STR_TO_DATE(Duration, '%H:%i:%s'),'00:00:00'))) AS average_duration_seconds FROM videos GROUP BY channel_name;'''
        mycursor.execute(q9)
        mydatabase.commit()
        output9 =mycursor.fetchall()
        df9= pd.DataFrame(output9,columns=["CHANNEL NAME","AVERAGE DURATION"])
        st.write(df9)

elif my_question=="10.Which videos have the highest number of comments, and what are their corresponding channel names?":
        q10='''SELECT Title AS video_title, channel_name as channelname, Comments AS num_comments 
         FROM videos 
         WHERE Comments IS NOT NULL 
         ORDER BY num_comments DESC;'''
        mycursor.execute(q10)
        mydatabase.commit()
        output10 =mycursor.fetchall()
        df10 = pd.DataFrame(output10,columns=["TITLE","CHANNEL NAME","HIGHEST COMMENTS"])
        st.write(df10)
        fig = go.Figure(
            go.Bar(
                x=df10['HIGHEST COMMENTS'],
                y=df10['CHANNEL NAME'],
                orientation='h',  
                marker_color='rgb(55, 83, 109)' 
            )
        )
        fig.update_layout(
            title='Channels with the Highest Number of Comments',
            xaxis_title="Number of Comments",
            yaxis_title="Channel Name",
            yaxis={'categoryorder': 'total ascending'},  # Order y-axis by total ascending
            plot_bgcolor='white'  # Set plot background color (optional)
        )
        st.plotly_chart(fig)
            

        


        




