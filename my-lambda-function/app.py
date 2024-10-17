import json
import os
import flywheel
import pandas as pd
import numpy as np

import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px

import csv
import io

import boto3
from boto3.dynamodb.conditions import Key

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

import fsspec
import requests


def search_file(service, file_name, folder_id):
    # Search for files by name in a specific folder
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files

def delete_file(service, file_id):
    # Delete file by file ID
    service.files().delete(fileId=file_id).execute()


def lambda_handler(event, context):
    
    dynamodb_client = boto3.client("dynamodb")
    dynamodb = boto3.resource("dynamodb")
    
    API = os.getenv("API_TOKEN")
    fw = flywheel.Client(api_key=API)
    message =  fw.get_current_user().email
    
    # Check user Info
    user_info = fw.get_current_user()
    
    print(f"Firstname: {user_info.firstname} \n"
        f"Lastname: {user_info.lastname} \n"
        f"Email: {user_info.email} \n"
        )
    
    # Retrieve sites data (this assumes 'sites' refer to some Flywheel data type - adjust accordingly)
    # For example, assume we're retrieving projects and filtering their location metadata:
    sites_cities = {'Accra':['Ghana (Accra)'],
                'Addis Ababa':['Ethiopia-BCD-Hyperfine','Ethiopia (ENAT)'],
                'Blantyre':['Malawi-Khula-Hyperfine'],'Bonn':['Bonn'],
                'Cape Town':['UCT-Khula-Hyperfine','UCT-D2-Hyperfine'], 
                'Dhaka':['Bangladesh (BEAN_EXT)','Bangladesh (BRAC Care Study)', 'Bangladesh (REVAMP)'],
                'Gaborone':['Botswana-MOTHEO'],
                'Harare':['Zimbabwe-Zvitambo'],
                'Kampala':['Uganda-PRIMES-Highfield', 'Uganda-PRIMES-Hyperfine'],
                'Karachi':['PRISMA-AKU'],'Kintampo':['PRISMA-Kintampo'],'Kisumu':['PRISMA-Kenya'],
                'London':['KCL-Neonatal-collection','KCL-HYPE'], 'Lucknow':[], 
                'Lusaka':['PRISMA-Zambia'], 'Nairobi':[],'Pretoria':['UP-Kalafong-Hyperfine'],
                'Soweto':['UP-Bara-Hyperfine'],
                'Vellore':['PRISMA-CMC'],'Zomba':['Malawi (REVAMP)']}
    
    print(sites_cities)
    # Retrieve sites data (this assumes 'sites' refer to some Flywheel data type - adjust accordingly)
    # For example, assume we're retrieving projects and filtering their location metadata
    df = pd.read_csv("unitySites.csv")
    #print(df)

    try:
        rows=[]
        for i , city in enumerate (sites_cities):
           
            site_scans = 0
            for project_label in sites_cities[city]:
                
                try:
                    project = fw.projects.find_one(f'label={project_label}')
                    site_scans += project['stats']['number_of']['sessions']
                    print(project_label,': ' , project['stats']['number_of']['sessions'])
                
                except Exception as e: 
                    print('Something went wrong', e)
    
            df.loc[df['city'] == city, 'scans'] = site_scans
    
            # Check if the value exists in the cities column
            if city in df['city'].values:
                # Update the cell in scans column
                df.loc[df['city'] == city, 'scans'] = site_scans
            else:
                # Create a dictionary for the new row
                new_row_data = {
                    'city': city,
                    'scans': site_scans
                }
                
                # Create a new DataFrame with NaN for all other columns
                new_row = pd.DataFrame({col: [np.nan] for col in df.columns}, index=[0])
                
                
                # Update the new row with the specified values
                new_row.update(pd.DataFrame(new_row_data, index=[0]))
    
                # Append the new row to the DataFrame
                df = pd.concat([df, new_row], ignore_index=True)
    
                # rows.append([city,site_scans])
    
         
    except Exception as e: 
        print('Something went wrong', e)
        
    # temp_csv_file = csv.writer(open("/tmp/site_scans.csv", "w+"))
    # # writing rows in to the CSV file
    
    # temp_csv_file.writerow(df.columns.tolist()) #header

    # for row in df:
    #     temp_csv_file.writerow(row)


    # s3.upload_file('/tmp/site_scans.csv', BUCKET_NAME,'site_scans.csv')
    

    # df = pd.DataFrame(columns=['city','n_scans'],data=rows)
    #df.to_csv("site_scans.csv",index=False)

    df["scans"] = df["scans"].apply(np.int64)

    write_csv(df)
    update_data()
    
    
    return {
        'statusCode': 200,
        'body': "Success"
    }

def update_data():
    
    # Load country data
    df = pd.read_csv("site_scans.csv")
    # Download the file
    url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    response = requests.get(url)

    # Save the downloaded zip file
    with open("ne_110m_admin_0_countries.zip", "wb") as f:
        f.write(response.content)
    
    world_data = gpd.read_file("ne_110m_admin_0_countries.zip")

        
    #url = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"
    #world_data = gpd.read_file(url)

    #world_data = gpd.read_file(gpd.datasets.get_path('naturalearth_lowres'))
    # world_data = pd.read_csv('/Users/nbourke/GD/atom/unity/beta/geo/world_data.csv')
    # Parse UNITY countries
    lst = ['United States of America', 'Pakistan', 'India', 'Zambia', 'Malawi', 'Uganda', 'Kenya', 'Botswana', 'Zimbabwe', 'Ghana', 'Ethiopia', 'South Africa', 'United Kingdom', 'Bangladesh', 'Sweden', 'Canada', 'Netherlands', 'Australia', 'Germany']
    world_data.columns = map(str.lower, world_data.columns)

    # for row in df.iterrows():
        # new_row['city_ascii'] = new_row_data['city']
        # new_row['population'] =
        # new_row['lng'] =

    #city_ascii,lat,lng,country,iso2,iso3,admin_name,capital,population,id,scans

    unity = world_data.loc[world_data['name'].isin(lst)]
    unity_json = json.loads(unity.to_json())

    # Setup country map
    fig1 = px.choropleth(unity,
                    geojson=unity_json,
                    featureidkey='properties.name',
                    locations='name',
                    color='name')

    ## Add labels on countries
    fig1.add_scattergeo(
        geojson=unity_json,
        locations=unity['name'],
        featureidkey='properties.name',
        text=unity['iso_a3'],
        mode='text',
    )
    
    city_lst = ['Karachi', 'Lucknow', 'Lusaka','Zomba', 'Blantyre', 'Kampala', 'Nairobi', 'Kisumu', 'Gaborone', 'Harare', 'Accra', 'Kintampo', 'Addis Ababa', 'Cape Town', 'Pretoria', 'London', 'Dhaka', 'Vellore', 'Bonn']
    city_data = df[df['city'].isin(city_lst)]

    city_data['text'] = df['city'] + ' Hyperfine scans: ' + city_data['scans'].astype(str)

    fig2 = go.Figure(data=go.Scattergeo(
                        lat=city_data['lat'],
                        lon=city_data['lng'],
                        text=city_data['text'],
                        mode='markers',
                        hoverinfo="text",
                        marker = dict(
                            size= 10, #city_data['scans'],
                            symbol = 'square',
                            opacity=0.8,
                            color='rgb(60, 211, 113)',
                            line = dict(
                            width=1,
                            color='rgba(102, 102, 102)'
                            ),
                        )))

    # Development sites
    DS_df = pd.read_csv('developmentSites.csv')
    DS_lst = ['Leiden', 'Lund', 'Vancouver','London', 'Los Angelas', 'Melbourne', 'Wisconsin']
    
    
    DS_data = DS_df[DS_df['city'].isin(DS_lst)]

    DS_data['text'] = DS_df['city'] + ' Research focus: ' + DS_data['scans'].astype(str)
    
    
    fig3 = go.Figure(data=go.Scattergeo(
                        lat=DS_data['lat'],
                        lon=DS_data['lng'],
                        text=DS_data['text'],
                        mode='markers',
                        hoverinfo='text',
                        marker = dict(
                            size= 10,
                            symbol = 'circle',
                            color= 'rgb(255,99,71)',
                            opacity=0.8,
                            line = dict(
                            width=1,
                            color= 'rgba(102, 102, 102)'
                            ),
                        )))
    
    fig = go.Figure(data = fig1.data + fig2.data + fig3.data)

    with open('unity_map.html', 'w') as f:
        f.write(fig.to_html(include_plotlyjs='cdn'))
        

    #fig.show()
    


def write_csv(df):
    
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    #s3 = boto3.client('s3')
    
    print(df)
    
    with open("site_scans.csv", "w", newline="") as f:
        temp_csv_file = csv.writer(f)
    
        # Write the header
        temp_csv_file.writerow(df.columns)
    
        for index, row in df.iterrows():
            # print('row: ', row)
            temp_csv_file.writerow(row)
            
    f.close()
    
    df.to_csv('site_scans.csv',index=False)
    #s3.upload_file('./tmp/site_scans.csv', BUCKET_NAME, 'site_scans.csv')
    file_id = update_drive()
    
    print(file_id)


def update_drive():
    
    try:
        # Load service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            'service-credentials-google.json'
        )
        
        # Build the Google Drive service
        service = build('drive', 'v3', credentials=credentials)

        ### UPLOAD FILES TO GDRIVE ###

        file_folder_dict={'site_scans.csv':'1WVRj-wu51QmkzeOYnWeLX0yi6qwEBbqa', 'unity_map.html':'1WTTQb7nKgOvnhLkt6EIQ6tlHEqf09luc'}

        for filename,folder_id in file_folder_dict.items():
            # File details
            file_metadata = {
                'name': filename,  # Name of the file to upload
                'parents': [folder_id]   # ID of the folder to upload to
            }
            media = MediaFileUpload('site_scans.csv')
        
            
             # Search for the file
            existing_files = search_file(service, filename, folder_id)
            if existing_files:
                # If the file exists, delete it
                for file in existing_files:
                    print(f"DELETING existing file: {file['name']} (ID: {file['id']})")
                    delete_file(service, file['id'])
            
            # Upload the file
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f'New file uploaded id {file.get('id')}')

        return {'successfully uploaded to drive'}
        
    except Exception as e:
        print("Failed to upload to drive: ", e)
        return e