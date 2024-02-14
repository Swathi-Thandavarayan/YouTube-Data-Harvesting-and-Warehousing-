
apiID = your api key
api_service_name = "youtube"
api_version = "v3"
youtube = googleapiclient.discovery.build(
      api_service_name, api_version, developerKey=apiID)

 

client = MongoClient('mongodb://localhost:27017/')
db = client["Youtubedata"]
collection = db['Channel details']



def  Channel_information(Channel_Id):
    request = youtube.channels().list(
    part="snippet,contentDetails,statistics,status",
    id=Channel_Id
    )
    response = request.execute()


    for i in range(0,len(response)):
        Channel_data_for_video = {"Channel_Name":response['items'][0]['snippet']['title'],
                        "Channel_Id":response['items'][0]['id'],
                        "Subscription_Count":response['items'][0]['statistics']['subscriberCount'],
                        "Channel_views":response['items'][0]['statistics']['viewCount'],
                        "Channel_Discription":response['items'][0]['snippet']['description'],
                        "playlist_Id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads'],
                        "videos_count":response['items'][0]['statistics']['videoCount'],
                        "channel_status":response['items'][0]['status']['privacyStatus']}


    #upload_playlist_Id
    playlist_Id = Channel_data_for_video.get('playlist_Id')
    
    for i in range(0,len(response)):
        Channel_data_for_playlist = {"Channel_Id":response['items'][0]['id'],
        "Channel_Name":response['items'][0]['snippet']['title'],
        "playlist_Id":response['items'][0]['contentDetails']['relatedPlaylists']['uploads']}

    #videos list
    video_Ids = []
    next_page_token = None
    while True:

            request = youtube.playlistItems().list(
            part = "snippet,contentDetails",
            maxResults = 50,
            pageToken = next_page_token,
            playlistId = playlist_Id
            )
            response = request.execute()
            for i  in range(0,len(response['items']),1):
                playlist_data_for_video = (response['items'][i]['contentDetails']['videoId'])
                video_Ids.append(playlist_data_for_video)
                next_page_token = response.get('nextPageToken')

            if next_page_token is None:
                break


    videos_list = []

    for i, video_id in enumerate(video_Ids):
            # Video details
            request_video = youtube.videos().list(
                part="snippet,contentDetails,statistics",
                id=video_id
            )
            response_video = request_video.execute()

            videos_details = {
                "video_Name": response_video['items'][0]['snippet']['title'],
                "channel_name":response_video['items'][0]['snippet']['channelTitle'],
                "video_id":response_video['items'][0]['id'],
                "Video_Description": response_video['items'][0]['snippet']['description'],
                "Tags": response_video['items'][0]['snippet'].get('tags'),
                "PublishedAt": response_video['items'][0]['snippet']['publishedAt'],
                "View_Count": response_video['items'][0]['statistics']['viewCount'],
                "Like_Count": response_video['items'][0]['statistics'].get('likeCount'),
                "Favorite_Count": response_video['items'][0]['statistics']['favoriteCount'],
                "Comment_Count": response_video['items'][0]['statistics'].get('commentCount'),
                "Duration": response_video['items'][0]['contentDetails']['duration'],
                "Thumbnail": response_video['items'][0]['snippet']['thumbnails']['default']['url'],
                "Caption_Status": response_video['items'][0]['contentDetails']['caption'],
                "Comments": []  }
            

            # Comments information
            try:
                    
                request_comments = youtube.commentThreads().list(
                    part="snippet,replies",
                    maxResults=20,
                    videoId=video_id
                )
                response_comments = request_comments.execute()
                

                for comment in response_comments.get('items', []):
                    comment_info = {
                        "Comment_id": comment.get('id'),
                        "Comment_Text": comment['snippet']['topLevelComment']['snippet'].get('textOriginal'),
                        "Comment_Author": comment['snippet']['topLevelComment']['snippet'].get('authorDisplayName'),
                        "Comment_PublishedAt": comment['snippet']['topLevelComment']['snippet'].get('publishedAt')
                    }
                    videos_details["Comments"].append(comment_info)
            except:
                None
            videos_list.append(videos_details)


    #mongodb insert commands
    record =({"Channel information":Channel_data_for_video,"playlist":Channel_data_for_playlist, "videos":videos_list })
    inserted_dictionary = collection.insert_one(record)
    #print(inserted_dictionary)
    inserted_id = inserted_dictionary.inserted_id
    #print(inserted_id)



    
config = {
    "user": "root",
    "password": "1234",
    "host": "127.0.0.1",
    "database": "youtubedata"
}

connection = mysql.connector.connect(**config)
cursor = connection.cursor()

def sqltables():

     # Drop and create the MySQL playlist table
    drop_playlist_query = '''DROP TABLE IF EXISTS playlist'''
    cursor.execute(drop_playlist_query)
    connection.commit()


     # Drop and create the MySQL channels table
    drop_channels_query = '''DROP TABLE IF EXISTS channels'''
    cursor.execute(drop_channels_query)
    connection.commit()

    create_channels_query = '''CREATE TABLE channels (
                                    Channel_Name VARCHAR(255),
                                    Channel_Id VARCHAR(255) PRIMARY KEY,
                                    Subscription_Count INT,
                                    Channel_views INT,
                                    Channel_Discription TEXT,
                                    playlist_Id VARCHAR(255),
                                    videos_count INT,
                                    channel_status VARCHAR(255)
                                )'''
    cursor.execute(create_channels_query)
    connection.commit()

    # Extract data from MongoDB for channels
    channels_list = []
    for data in collection.find({}, {"_id": 0, "Channel information": 1}):
        channels_list.append(data["Channel information"])

    df_channels = pd.DataFrame(channels_list)

    # Insert data into MySQL channels table
    for index, row in df_channels.iterrows():
        insert_channels_query = '''INSERT INTO channels (
                                        Channel_Name,
                                        Channel_Id,
                                        Subscription_Count,
                                        Channel_views,
                                        Channel_Discription,
                                        playlist_Id,
                                        videos_count,
                                        channel_status
                                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s)'''
        
        values_channels = (row['Channel_Name'], 
                        row['Channel_Id'], 
                        row['Subscription_Count'], 
                        row['Channel_views'], 
                        row['Channel_Discription'], 
                        row['playlist_Id'], 
                        row['videos_count'], 
                        row['channel_status'])
        
        try:
            cursor.execute(insert_channels_query, values_channels)
            connection.commit()
        except Exception as e:
            print(f"Error executing query: {e}")


   

    create_playlist_query = '''CREATE TABLE playlist (
                                    Channel_Id VARCHAR(255) PRIMARY KEY,
                                    Channel_Name VARCHAR(255),
                                    playlist_Id VARCHAR(255)
                                    
                                )'''
    cursor.execute(create_playlist_query)
    connection.commit()

    # Extract data from MongoDB for playlist
    playlist_list = []
    for data in collection.find({}, {"_id": 0, "playlist": 1}):
        
        playlist_list.append(data["playlist"])

    df_playlist = pd.DataFrame(playlist_list)

    # Insert data into MySQL playlist table
    for index, row in df_playlist.iterrows():
        insert_playlist_query = '''INSERT INTO playlist (
                                        Channel_Id,
                                        Channel_Name,
                                        playlist_Id
                                    ) VALUES (%s, %s, %s)'''
        
        values_playlist = (row['Channel_Id'], 
                        row['Channel_Name'],
                        row['playlist_Id'])
        
        try:
            cursor.execute(insert_playlist_query, values_playlist)
            connection.commit()
        except:
            print("Already executed")

    # drop table if exists videos table
    drop_video_query = '''DROP TABLE IF EXISTS videos'''
    cursor.execute(drop_video_query)
    connection.commit()

    # create table in MySQL videos table
    create_videos_query = '''CREATE TABLE videos (
        video_Name VARCHAR(255),
        channel_name VARCHAR(255),
        video_id VARCHAR(255) PRIMARY KEY,
        Video_Description TEXT,
        Tags TEXT,
        PublishedAt TIMESTAMP,
        View_Count BIGINT,
        Like_Count BIGINT,
        Favorite_Count INT,
        Comment_Count INT,
        Duration BIGINT,
        Thumbnails VARCHAR(255),
        Caption_Status VARCHAR(20),
        Comments TEXT
        
    )'''
    cursor.execute(create_videos_query)
    connection.commit()

    # extract data from MongoDB videos table
    video_list = []
    for video_data in collection.find({}, {"_id": 0, "videos": 1}):
        if 'videos' in video_data and isinstance(video_data['videos'], list):
            video_list.extend(video_data["videos"])

    df_videos = pd.DataFrame(video_list)
    df_videos['PublishedAt'] = pd.to_datetime(df_videos['PublishedAt'], format='%Y-%m-%dT%H:%M:%SZ', errors='coerce')


    def convert_duration(Duration):
        if pd.notnull(Duration):
            Duration = Duration.upper()  
            seconds = 0
            seconds_min = 0  

            if 'PT' in Duration:
                Duration = Duration[2:]  # Remove 'PT'

            if 'M' in Duration:
                try:
                    minutes = int(Duration.split('M')[0])
                    seconds_min = minutes * 60
                    
                except ValueError:
                    pass  

            if 'S' in Duration:
                
                if 'M' in Duration:
                            Duration = Duration.split('M')[1]
                            seconds = int(Duration.split('S')[0])
                
                else:
                    seconds = int(Duration.split('S')[0])


        result = seconds_min + seconds
        
        return result

    df_videos['Duration'] = df_videos['Duration'].apply(convert_duration)

    # Convert the list columns to strings
    df_videos['Comments'] = df_videos['Comments'].apply(lambda x: ', '.join(map(str, x)) if x else '')
    df_videos['Tags'] = df_videos['Tags'].apply(lambda x: ', '.join(map(str, x)) if x else '')



    # insert to MySQL videos table
    for index, row in df_videos.iterrows():
        insert_query_videos = '''INSERT INTO videos (
            video_Name,
            channel_name,
            video_id,
            Video_Description,
            Tags,
            PublishedAt,
            View_Count,
            Like_Count,
            Favorite_Count,
            Comment_Count,
            Duration,
            Thumbnails,
            Caption_Status,
            Comments
        ) VALUES (%s,%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)'''

        values_videos = (
            row['video_Name'],
            row['channel_name'],
            row['video_id'],
            row['Video_Description'],
            row['Tags'],
            row['PublishedAt'],
            row['View_Count'],
            row['Like_Count'],
            row['Favorite_Count'],
            row['Comment_Count'],
            row['Duration'],
            row['Thumbnail'],
            row['Caption_Status'],
            row['Comments']
        )

        try:
            cursor.execute(insert_query_videos, values_videos)
            connection.commit()
        
        except Exception as e:
            print(f"Error: {e}")

        # Drop and create the MySQL comments table
    drop_query = '''DROP TABLE IF EXISTS comments'''
    cursor.execute(drop_query)
    connection.commit()

    create_comments_query = '''CREATE TABLE comments (
                            comment_id VARCHAR(255) PRIMARY KEY,
                            comment_text text,
                            comment_author VARCHAR(255),
                            comment_publishedat text
                        )'''
    cursor.execute(create_comments_query)
    connection.commit()

    #extract data from Mongodb for comments
    comments_list = []
    for channel_data in collection.find({}, {"_id": 0, "videos": 1}):
        for video_entry in channel_data.get("videos", []):
            for comment_data in video_entry.get("Comments", []):
                comments_list.append(comment_data)

    df = pd.DataFrame(comments_list)
    for index,row in df.iterrows():
            
            insert_query = '''INSERT INTO comments(comment_id,
                                                comment_text ,
                                                comment_author ,
                                                comment_publishedat
                                                ) VALUES(%s,%s,%s,%s)'''   
            
            values = (row['Comment_id'],
                    row['Comment_Text'],
                    row['Comment_Author'],
                    row['Comment_PublishedAt']
                    )
            try:
                    cursor.execute(insert_query,values)
                    connection.commit()

            
            except Exception as e:
                print(f"Error: {e}")



## STREAMLIT
st.set_page_config(
    page_title="YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit",
    page_icon="🔗",
    layout="wide",  # "wide" or "centered"
    initial_sidebar_state="collapsed"  # "auto" or "expanded" or "collapsed"
    
)

st.title("YouTube Data Harvesting and Warehousing using SQL, MongoDB and Streamlit")
st.markdown("Welcome to project YDHW_by Swathi:smile:")
st.markdown("The app that helps you to retrieve and analyse data from various Youtube , responding to various queries. ")
selected_page = st.sidebar.selectbox("Select Page", ["CHANNEL DETAILS", "MONGODB","MYSQL"])

def SQL_QUERIES():
        st.header("MySQL Queries")
        sql_queries = ["1.What are the names of all the videos and their corresponding channels?",
                                                   "2.Which channels have the most number of videos, and how many videos do they have?",
                                                    "3.What are the top 10 most viewed videos and their respective channels?",
                                                    "4.How many comments were made on each video, and what are their corresponding video names?",
                                                    "5.Which videos have the highest number of likes, and what are their corresponding channel names?",
                                                    "6.What is the total number of likes for each video and what are their corresponding video names?",
                                                    "7.What is the total number of views for each channel and what are their corresponding channel names?",
                                                    "8.What are the names of all the channels that have published videos in the year 2022?",
                                                    "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?",
                                                    "10.Which videos have the highest number of comments, and what are their corresponding channel names?"]
        queries =st.selectbox("select your query",sql_queries)


        if queries == "1.What are the names of all the videos and their corresponding channels?":
            query1 = '''SELECT video_Name as VideoName,channel_name  FROM videos '''
            cursor.execute(query1)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=['VideoName', 'channel_name'])
            df = df.sort_values(by='channel_name')
            st.dataframe(df)
            
        elif queries == "2.Which channels have the most number of videos, and how many videos do they have?":
            query2 = '''SELECT Channel_Name ,videos_count FROM channels
                        order by videos_count desc'''
            cursor.execute(query2)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_Name', 'videos_count'])
            st.dataframe(df)

        elif queries == "3.What are the top 10 most viewed videos and their respective channels?":
            query3 = '''SELECT video_Name as VideoName, channel_name as ChannelName, View_Count  FROM videos
                        where View_Count is not null
                        order by View_Count desc limit 10'''    
            cursor.execute(query3)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['VideoName', 'ChannelName', 'View_Count'])
            #df_top_10 = df.head(10)
            st.dataframe(df)

        elif queries == "4.How many comments were made on each video, and what are their corresponding video names?":
            query4 = '''SELECT video_Name as VideoName, Comment_Count FROM videos
                        where Comment_Count is not null'''
            cursor.execute(query4)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['VideoName', 'Comment_Count'])
            st.dataframe(df)

        elif queries == "5.Which videos have the highest number of likes, and what are their corresponding channel names?":
            query5 = '''SELECT channel_name, video_Name as VideoName, Like_Count FROM videos
                        where Like_Count is not null
                        order by Like_Count desc'''
            cursor.execute(query5)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['channel_name','VideoName', 'Like_Count'])
            st.dataframe(df)

        elif queries == "6.What is the total number of likes for each video and what are their corresponding video names?":
            query6 = '''SELECT video_Name as VideoName, Like_Count FROM videos
                        where Like_Count is not null'''
            cursor.execute(query6)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['VideoName', 'Like_Count'])
            st.dataframe(df)

        elif queries == "7.What is the total number of views for each channel and what are their corresponding channel names?":
            query7 = '''SELECT Channel_Name ,Channel_views FROM channels'''
            cursor.execute(query7)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['VideoName', 'Channel_views'])
            st.dataframe(df)

        elif queries == "8.What are the names of all the channels that have published videos in the year 2022?":
            query8 = '''SELECT Channel_Name, video_Name,PublishedAt as pulishedDate FROM videos
                        WHERE PublishedAt >= '2022-01-01 00:00:00' AND PublishedAt < '2023-01-01 00:00:00' '''
            cursor.execute(query8)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['Channel_Name','VideoName', 'pulishedDate'])
            st.dataframe(df)
            
        elif queries == "9.What is the average duration of all videos in each channel, and what are their corresponding channel names?":
            query9 = '''SELECT DISTINCT channel_name , AVG(Duration) AS Average_Duration_Seconds FROM videos
                        GROUP BY channel_name'''
            cursor.execute(query9)
            results = cursor.fetchall()
            df = pd.DataFrame(results, columns=['Channel_Name', 'Average_Duration_Seconds'])
            st.dataframe(df)

            
        elif queries == "10.Which videos have the highest number of comments, and what are their corresponding channel names?":
            query10 = '''SELECT channel_name, video_Name as VideoName, Comment_Count FROM videos
                        where Comment_Count is not null
                        order by Comment_Count desc'''
            cursor.execute(query10)
            results = cursor.fetchall() 
            df = pd.DataFrame(results,columns=['channel_name','VideoName', 'Comment_Count'])
            st.dataframe(df)
def insert_data(channel_id):
    # Check if document with the given channel_id already exists
    existing_document = collection.find_one({"Channel information.Channel_Id": channel_id})

    if existing_document:
        # Document already exists, update it with new data
        st.warning(f"Data for channel_id {channel_id} already exists. Updating...")

        # Replace the following line with the new data you want to update the document with
        new_data = {"Channel information.Channel_Id": channel_id}

        # Use update_one to update the existing document with new data
        collection.update_one({"Channel information.Channel_Id": channel_id}, {"$set": new_data})

        st.success(f"Data for channel_id {channel_id} successfully updated in MongoDB.")

    else:
        # Document doesn't exist, insert new data
        Channel_information(channel_id)
        st.success(f"Data for channel_id {channel_id} successfully uploaded to MongoDB.")


if selected_page == "CHANNEL DETAILS":
    st.markdown('''This projects aims to create a Streamlit application to 
                create a simple UI where users can enter a YouTube channel ID,
                 view the channel details, and select channels to migrate to the data warehouse''')

    with st.sidebar:
        st.title("CHANNEL DETAILS")
        st.markdown("""To view Channel Details enter your CHANNEL ID
                    upload to Mongodb and mysql to process the Data """)
        channel_id = st.text_input("Enter your Channel_id")
        with st.expander("HOW TO GET CHANNEL ID"):
            st.markdown("""
                1. Visit the YouTube channel.
                2. Click on the "About" tab, usually located below the channel banner.
                3. The Channel ID may be visible.
            """)

        if st.button("Upload Data to MongoDB"):
                insert_data(channel_id)

        st.markdown('''About:
                    By entering the ChannelID we use the YouTube API to retrieve channel and video data,
                    using the Google API client library for Python to make requests to the API and fetch
                    the required details for our detailed analysis.''')
        st.markdown('''Channel Details of the entered ChannelID are uploaded to Mongodb,
                    if you enter an exisiting ChannelID the details are updated.''')
        

    st.write("Existing Data From Mysql")
    options_to_select = ["Channels", "Playlist", "Videos", "Comments"]
    view_query = st.radio("select an option", options_to_select)

    if view_query == "Channels":
        insert_query = '''SELECT * FROM channels
                        ORDER BY Channel_Name'''
        cursor.execute(insert_query)
        results = cursor.fetchall()
        df = pd.DataFrame(results,columns=['Channel_Name', 'Channel_Id','Subscription_Count','Channel_views','Channel_Discription','playlist_Id','videos_count','channel_status'])
        st.dataframe(df)

    elif view_query == "Playlist":
        insert_query = '''SELECT * FROM playlist
                        ORDER BY Channel_Name'''
        cursor.execute(insert_query)
        results = cursor.fetchall()
        df = pd.DataFrame(results,columns=['Channel_Id', 'Channel_Name','playlist_Id'])
        st.dataframe(df)

    elif view_query == "Videos":
        insert_query = '''SELECT * FROM videos
                        ORDER BY Channel_Name'''
        cursor.execute(insert_query)
        results = cursor.fetchall()
        df = pd.DataFrame(results,columns=['VideoName', 'Channel_Name','video_id','Video_Description','Tags','PublishedAt','View_Count','Like_Count','Favorite_Count','Comment_Count','Duration','Thumbnails','Caption_Status','Comments'])
        st.dataframe(df)

    elif view_query == "Comments":
        insert_query = '''SELECT * FROM comments'''                
        cursor.execute(insert_query)
        results = cursor.fetchall()
        df = pd.DataFrame(results,columns=['comment_id', 'comment_text','comment_author','comment_publishedat'])
        st.dataframe(df)


elif selected_page == "MONGODB": 

   channel_names = collection.distinct("Channel information.Channel_Name")
   options = ["Select All"] + channel_names
   selected_channel = st.sidebar.multiselect("Select a channel", options)
   
   if st.sidebar.button('Upload Selected Options to Mysql'):
    if "Select All" in selected_channel:
        selected_channel.remove("Select All")
        sqltables()
        st.sidebar.write("upload successfull")
    
    if selected_channel in options:
            # Modify the query to get data based on the selected option
            sqltables()

        
    st.sidebar.markdown('''Multiple Channels can be selected from the options to migrate data to MYSQL, 
                        to migrate all select (SELECT ALL)''')
    st.sidebar.markdown('''Selected channels will be viewed after uploading for you reference''')
    st.sidebar.markdown('''Once the data is retrieved from the YouTube API,
                 It is store it in a MongoDB data lake. 
                MongoDB is a great choice for a data lake because it can handle unstructured and semi-structured data easily,
                plus Data retrieval is rapid.The processed data is migrated to Mysql to a structured form''')

    
elif selected_page == "MYSQL":

    SQL_QUERIES()
    st.sidebar.markdown('''SQL queries are used to join the tables in the SQL data warehouse and 
                        retrieve data for specific channels based on user input. 
                        The Structured data is then handled qith queries to retrieve nessecary data''')
    st.sidebar.markdown('''Various queries have been handled for your convinience ..further Channels added 
                        also will be appended to the queries''')
