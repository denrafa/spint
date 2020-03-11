from datetime import time
from typing import Dict, List, Tuple, Any
import sqlite3
import feedparser
import webbrowser
import ssl
import pandas as pd
import plotly.express as px
import requests
from geopy import location
from geopy.exc import GeocoderTimedOut
from geotext import GeoText
import plotly.graph_objects as go
import numpy as np
import plotly.express as px
from sqlalchemy import create_engine
from geopy.extra.rate_limiter import RateLimiter
#from geopandas.tools import geocode
#import geocode

if hasattr(ssl, '_create_unverified_conext'):
    ssl._create_default_https_context = ssl._create_default_https_context


def get_stackoverflow_jobs():
    entries = []
    feed = feedparser.parse("https://stackoverflow.com/jobs/feed")
    feed_entries = feed.entries
    for e in feed_entries:
        entries.append(e)
    return entries


def get_github_jobs_data() -> List[Dict]:
    """retrieve github jobs data in form of a list of dictionaries after json processing"""
    all_data = []
    page = 1
    more_data = True
    while more_data:
        url = f"https://jobs.github.com/positions.json?page={page}"
        raw_data = requests.get(url)
        if "GitHubber!" in raw_data.text:  # sometimes if I ask for pages too quickly I get an error; only happens in testing
            continue  # trying continue, but might want break
        if not raw_data.ok:  # if we didn't get a 200 response code, don't try to decode with .json
            continue
        partial_jobs_list = raw_data.json()
        all_data.extend(partial_jobs_list)
        if len(partial_jobs_list) < 50:
            more_data = False
#        time.sleep(.1)  # short sleep between requests so I dont wear out my welcome.
        page += 1
    return all_data


def save_data(data, filename='data.txt'):
    with open(filename, 'a', encoding='utf-8') as file:
        for item in data:
            print(item, file=file)


def hard_code_create_table(cursor: sqlite3.Cursor):
    create_statement = f"""CREATE TABLE IF NOT EXISTS hardcode_github_jobs(
    id TEXT PRIMARY KEY,
    type TEXT,
    url TEXT,
    created_at TEXT,
    company TEXT NOT NULL,
    company_url TEXT,
    location TEXT,
    title TEXT NOT NULL,
    description TEXT,
    how_to_apply TEXT,
    company_logo TEXT
    );
        """
    cursor.execute(create_statement)

    create_statement1 = f"""CREATE TABLE IF NOT EXISTS  stackoverflow_jobs(
       guid int PRIMARY KEY,
       link TEXT,
       date TEXT,
       title TEXT NOT NULL,
       description TEXT
       );
           """
    cursor.execute(create_statement1)

    create_statement2 = f"""CREATE TABLE IF NOT EXISTS  location(
       name TEXT,
       latitude TEXT,
       longitude TEXT,
          
        );
            """
    cursor.execute(create_statement2)


def hard_code_save_to_db(cursor: sqlite3.Cursor, all_github_jobs: List[Dict[str, Any]], all_stackoverflow_job,
                         all_location):
    # in the insert statement below we need one '?' for each column, then we will use a second param with each of the values
    # when we execute it. SQLITE3 will do the data sanitization to avoid little bobby tables style problems
    insert_statement = f"""INSERT INTO hardcode_github_jobs(
        id, type, url, created_at, company, company_url, location, title, description, how_to_apply, company_logo)
        VALUES(?,?,?,?,?,?,?,?,?,?,?)"""
    for job_info in all_github_jobs:
        # first turn all the values from the jobs dict into a tuple
        data_to_enter = tuple(job_info.values())
        cursor.execute(insert_statement, data_to_enter)
    sql = ''' INSERT INTO  stackoverflow_jobs(guid,link,date,title,description)
    
    
                      VALUES(?,?,?,?,?) '''
    for job_info in all_stackoverflow_job:
        print(job_info)
        if not 'created_at' in job_info:
            job_info['created_at'] = job_info['published']
        data_enter = (job_info['guid'], job_info['link'], job_info['created_at'], job_info['title'], job_info['description'])
        cursor.execute(sql, data_enter)


    sql1 = "SELECT title, FROM stackoverflow_jobs;"
    cursor.execute(sql1)
    cursor.fetchone()
    cursor.fetchall()
    for result in cursor.execute(sql1):
         print(result)



    sql2 = ''' INSERT INTO  location(name,latitude,longitude)


                         VALUES(?,?,?) '''
    for job_info in all_location:
        print(job_info)
        if not 'created_at' in job_info:
            job_info['created_at'] = job_info['published']
        data_enter1 = (
        job_info['name'], job_info['latitude'], job_info['longitude'])
        cursor.execute(sql, data_enter1)

        sql3 = "SELECT title, FROM location;"
        cursor.execute(sql3)
        cursor.fetchone()
        cursor.fetchall()
        for result in cursor.execute(sql3):
            print(result)
#filtering the rows
# df = pd.read_sql_query('SELECT company, url, location'
 #                      'FROM  hardcode_github_jobs'
  #                     'WHERE location = "NEW YORK"')




def map():
    #us_cities = pd.read_csv("https://raw.githubusercontent.com/plotly/datasets/master/us-cities-top-1k.csv")

    """SELECT title, description, location, latitude * FROM stackoverflow_jobs INNER JOIN  location_table ON stackoverflow_jobs.location_table , location = locations_table.location;"""
    lats = []
    lons = []
    titles =[]
    firstquery = """SELECT title, description, location, latitude, longitude FROM hardcode_github_jobs INNER JOIN  location_table ON hardcode_github_jobs.location  = locations_table.location;"""
    #now execute first query
    #then for loop through results

    fig = px.scatter_mapbox( lat=lats, lon=lons, hover_name="City", hover_data=titles,
                        color_discrete_sequence=["fuchsia"], zoom=3, height=300)
    fig.update_layout(
     mapbox_style="white-bg",
            mapbox_layers=[
        {
            "below": 'traces',
            "sourcetype": "raster",
            "source": [
                "https://basemap.nationalmap.gov/arcgis/rest/services/USGSImageryOnly/MapServer/tile/{z}/{y}/{x}"
            ]
        }
      ])
    fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})
    fig.show()




# filtering data
'''''
    us_cities.columns =[column.replace(" ", "_") for column in us_cities.columns]

   # filtering with query method
    us_cities.query('Senior_Management == True', inplace=True)

# display
    us_cities

#dataframe
df2 = pd.DataFrame(np.array([[1, 2, 3], [4, 5, 6], [7, 8, 9]]),
                   columns=['a', 'b', 'c'])
'''






class AtributeError(object):
    pass


def do_geocode(param):
    pass


def insert_location_into_job_location_db(cursor, param, latitude, longitute):
    pass


def get_location_table(cursor: object) -> object: #, jobs_Location_list,):
  #  conn, cursor = open_db('jobdemo.sqlite')
    jobs_Location_list = cursor.execute("SELECT location FROM hardcode_github_jobs")
    for item in jobs_Location_list :
         print("trying to fing remote in: " + str(item))
         if str(item[0].find("remote")) != "-1":
            print("this is a remote job")
         else:
              dblocation = cursor.execute("SELECT name FROM location WHERE name = 'item  ' " )
              location_found = dblocation.fetchone()

         if len (location_found) > 0:
          print("we found" + item[3] + "in the databse.")
         else:
                      print(item[3] + " does not exit ! inserting into database.")
                      try:
                          latitude, longitute = do_geocode(item[0])
                          insert_location_into_job_location_db(cursor,item[0], latitude, longitute)
                      except GeocoderTimedOut:
                          print("GEOCODER TIMEDOUT ! BEGIN RECURSIVE FUNCTION.")
                          latitude, longitute = do_geocode(item[3])
                          insert_location_into_job_location_db(cursor,item[0], latitude, longitute)
                      except AtributeError:
                          print("not a valid location or unknown location.")
    #jobdemo.close_db(conn)
    print()


# making list to be filter
def company_list(cursor,hardcode_github_jobs):
    list= []







def open_db(filename: str) -> Tuple[sqlite3.Connection, sqlite3.Cursor]:
    db_connection = sqlite3.connect(filename)  # connect to existing DB or create new one
    cursor = db_connection.cursor()  # get ready to read/write data
    return db_connection, cursor


def close_db(connection: sqlite3.Connection):
    connection.commit()  # make sure any changes get saved
    connection.close()


def main() -> object:
    jobs_table_name = 'github_jobs_table'  # might be better as a constant in global space
    jobs_table1_name = 'stackoverflow_jobs'
    jobs_table2_name = 'location'
    db_name = 'jobdemo.sqlite'
    connection, cursor = open_db(db_name)
    data = get_github_jobs_data()
    data1 = get_stackoverflow_jobs()
    data2 = get_location_table()
    print(data)
    print(data1)
    print(data2)
    #entries = get(cursor)
    hard_code_create_table(cursor)
#    get_location_table(cursor)

    hard_code_save_to_db(cursor, data, data1, data2)



    #read_from_db(cursor)
    # desc = make_column_description_from_json_dict(data[0])  # assumes all records have the same fields so make table from first
    # create_table(cursor, desc, jobs_table_name)
    # save_to_db(data, cursor, jobs_table_name)
    close_db(connection)


if __name__ == '__main__':
    #db_name = 'jobdemo.sqlite'
    #connection, cursor = open_db(db_name)
    #location_table(cursor)
    #close_db(connection)
    main()
