import json
import os
import flywheel
import pandas as pd
import numpy as np
import typing as t

import geopandas as gpd
import plotly.graph_objects as go
import plotly.express as px

from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# import fsspec
import requests

### Global variables: ###
# Dictionary relating cities to the corresponding Flywheel project labels:
SITES_CITIES = {
    'Accra': ['Ghana (Accra)'],
    'Addis Ababa': ['Ethiopia-BCD-Hyperfine', 'Ethiopia (ENAT)'],
    'Blantyre': ['Malawi-Khula-Hyperfine'],
    'Bonn': ['Bonn'],
    'Cape Town': ['UCT-Khula-Hyperfine', 'UCT-D2-Hyperfine'],
    'Dhaka': ['Bangladesh (BEAN_EXT)', 'Bangladesh (BRAC Care Study)', 'Bangladesh (REVAMP)'],
    'Gaborone': ['Botswana-MOTHEO'],
    'Harare': ['Zimbabwe-Zvitambo'],
    'Kampala': ['Uganda-PRIMES-Highfield', 'Uganda-PRIMES-Hyperfine'],
    'Karachi': ['PRISMA-AKU'],
    'Kintampo': ['PRISMA-Kintampo'],
    'Kisumu': ['PRISMA-Kenya'],
    'London': ['KCL-Neonatal-collection', 'KCL-HYPE'],
    'Lucknow': [],
    'Lusaka': ['PRISMA-Zambia'],
    'Nairobi': [],
    'Pretoria': ['UP-Kalafong-Hyperfine'],
    'Soweto': ['UP-Bara-Hyperfine'],
    'Vellore': ['PRISMA-CMC'],
    'Zomba': ['Malawi (REVAMP)']
}
# Missing: ['Los Angeles', 'Melbourne']
DEVELOPMENT_CITIES = {
    'Leiden': ['LUMC-Lowfield'],
    'Lund': ['Lund (High Field)', 'Lund (Low Field)'],
    'Vancouver': ['UBC'],
    'London': ['KCL-Neonatal-collection', 'KCL-HYPE', 'KCL-STH1'],
    'Wisconsin': ['UWisc'],
}
# This is either the URL or the path to the GeoJSON file with the world data source
WORLD_DATA_SRC = "https://naciscdn.org/naturalearth/110m/cultural/ne_110m_admin_0_countries.zip"


def search_file(service, file_name, folder_id):
    # Search for files by name in a specific folder
    query = f"name='{file_name}' and '{folder_id}' in parents and trashed=false"
    results = service.files().list(q=query, fields="files(id, name)").execute()
    files = results.get('files', [])
    return files


def delete_file(service, file_id):
    # Delete file by file ID
    service.files().delete(fileId=file_id).execute()


def get_site_scans(fw_client: flywheel.Client, projects: t.List[str]) -> int:
    """Get the number of scans accross a list of projects.

    Args:
        fw_client (flywheel.Client): The Flywheel SDK client.
        projects (t.List[str]): The list of project labels.

    Returns:
        int: The number of scans.
    """
    site_scans = 0
    for project_label in projects:
        try:
            project = fw_client.projects.find_one(
                f'label={project_label}', exhaustive=True
            )
            site_scans += project['stats']['number_of']['sessions']
            print(project_label,': ' , project['stats']['number_of']['sessions'])
        
        except Exception as e: 
            print(project_label,': Something went wrong', e)

    return site_scans


def update_number_of_scans_in_csv(
    fw_client: flywheel.Client,
    cities_dict: t.Dict[str, t.List[str]],
    csv_path: str
) -> None:
    """Update the number of scans in the CSV file with the data in Flywheel
    
    It retrieves the number of scans for each site (city) in the cities_dict and
    updates the CSV file.

    Args:
        fw_client (flywheel.Client): The Flywheel SDK client.
        cities_dict (t.Dict[str, t.List[str]]): The dictionary with the cities and
            the corresponding Flywheel project labels.
        csv_path (str): The path to the CSV file.
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"File not found: {csv_path}")

    df = pd.read_csv(csv_path)

    try:
        rows=[]
        for city, projects_list in cities_dict.items():
            site_scans = get_site_scans(fw, projects_list)
    
            # Check if the value exists in the cities column
            if city in df['city'].values:
                # Update the cell in scans column
                df.loc[df['city'] == city, 'scans'] = site_scans
            else:
                # Create new DataFrame for the new row, with NaN for all other columns
                new_row = pd.DataFrame([{
                    'city': city,
                    'scans': site_scans,
                    **{col: np.nan for col in df.columns if col not in ["city", "scans"]}
                }])
    
                # Append the new row to the DataFrame
                df = pd.concat([df, new_row], ignore_index=True)    

    except Exception as e: 
        print('Something went wrong', e)
    
    df["scans"] = df["scans"].apply(np.int64)

    # Save the updated DataFrame to the CSV file:
    df.to_csv(csv_path, index=False)


def main(fw):
    # Check URL:
    print(f"Site URL: {fw.get_config().site.api_url.removesuffix("/api")}")

    # Check user Info
    user_info = fw.get_current_user()
    
    print(f"Firstname: {user_info.firstname} \n"
        f"Lastname: {user_info.lastname} \n"
        f"Email: {user_info.email} \n"
    )

    # Get data from Flywheel
    # A) For the SITE_CITIES (data-contributing sites):
    # print(SITES_CITIES)
    sites_csv_path = "site_scans.csv"    
    update_number_of_scans_in_csv(fw, SITES_CITIES, sites_csv_path)
    # write_csv_to_bucket(sites_csv_path)

    # B) For the DEVELOPMENT_CITIES:
    # print(DEVELOPMENT_CITIES)
    dev_sites_csv_path = "developmentSites.csv"
    update_number_of_scans_in_csv(fw, DEVELOPMENT_CITIES, dev_sites_csv_path)

    # Generate the map figure
    update_map_figure(WORLD_DATA_SRC, sites_csv_path, dev_sites_csv_path)

    return {
        'statusCode': 200,
        'body': "Success"
    }


def update_map_figure(map_file: str, sites_csv_path: str, dev_sites_csv_path: str) -> None:
    """Update the map figure with the data from the CSV files.

    Args:
        map_file (str): The path to the map file.
        sites_csv_path (str): The path to the CSV file with the data-contributing sites.
        dev_sites_csv_path (str): The path to the CSV file with the development sites.
    """
    
    # Load country data
    world_data = gpd.read_file(map_file)
    world_data.columns = map(str.lower, world_data.columns)

    # Load the CSV files:
    city_data = pd.read_csv(sites_csv_path)
    DS_data = pd.read_csv(dev_sites_csv_path)

    ### Base layer: ###
    # World map with UNITY countries highlighted in colors

    # Parse UNITY countries (grab countries present in either city_data or DS_data)
    unity = world_data.loc[
        world_data["name"].isin(city_data["country"])
        | world_data["name"].isin(DS_data["country"])
    ]
    unity_json = json.loads(unity.to_json())

    # Setup country map
    fig = px.choropleth(
        unity,
        geojson=unity_json,
        featureidkey='properties.name',
        locations='name',
        color='name'
    )

    ## Add labels on countries
    fig.add_scattergeo(
        name='Number of scans',
        geojson=unity_json,
        locations=unity['name'],
        featureidkey='properties.name',
        text=unity['iso_a3'],
        mode='text',
    )

    ### Layer 1: data-contributing sites ###
    # city_lst = ['Karachi', 'Lucknow', 'Lusaka','Zomba', 'Blantyre', 'Kampala', 'Nairobi', 'Kisumu', 'Gaborone', 'Harare', 'Accra', 'Kintampo', 'Addis Ababa', 'Cape Town', 'Pretoria', 'London', 'Dhaka', 'Vellore', 'Bonn']
    # city_data = city_data[city_data['city'].isin(city_lst)]
    # city_data['text'] = df['city'] + ' Hyperfine scans: ' + city_data['scans'].astype(str)

    data_contributing_sites_scatter = go.Scattergeo(
        name='Data-contributing sites',
        lat=city_data['lat'],
        lon=city_data['lng'],
        text=city_data['scans'],
        mode='markers+text',
        marker = dict(
            # Adjust size by the square root of "scans", so that the marker area is
            # proportional to the number of scans
            size= city_data['scans'] ** 0.5 * 2,
            symbol = 'square',
            opacity=0.8,
            color='rgb(60, 211, 113)',
            line = dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
        ),
        textposition="middle center",
        hoverinfo="location",
    )

    # Development sites
    # DS_lst = ['Leiden', 'Lund', 'Vancouver','London', 'Los Angelas', 'Melbourne', 'Wisconsin']
    # DS_data['text'] = DS_df['city'] + ' Research focus: ' + DS_data['scans'].astype(str)

    development_sites_scatter = go.Scattergeo(
        name='Development sites',
        lat=DS_data['lat'],
        lon=DS_data['lng'],
        text=DS_data['scans'],
        mode='markers+text',
        marker = dict(
            # Adjust size by the square root of "scans", so that the marker area is
            # proportional to the number of scans
            size= DS_data['scans'] ** 0.5 * 2,
            symbol = 'circle',
            opacity=0.8,
            color='rgb(255, 99, 71)',
            line = dict(
                width=1,
                color='rgba(102, 102, 102)'
            ),
        ),
        textposition="middle center",
        hoverinfo="text",
    )
    
    fig.add_traces([data_contributing_sites_scatter, development_sites_scatter])
    # with open('unity_map.html', 'w') as f:
    #     f.write(fig.to_html(include_plotlyjs='cdn'))
        
    fig.show()
    


def write_csv_to_bucket(csv_path: str) -> None:
    
    BUCKET_NAME = os.getenv("BUCKET_NAME")
    # TO-DO: call a different method to upload the file depending on the bucket type
    # For now, we are just calling the Google Drive method:
    file_id = update_google_drive(csv_path)
    print(file_id)


def update_s3_bucket(file_path: str) -> str:
    """Upload the file to an S3 bucket.
    
    Arguments:
        file_path {str} -- The path to the file to upload.
    """

    #s3 = boto3.client('s3')

    #s3.upload_file('./tmp/site_scans.csv', BUCKET_NAME, 'site_scans.csv')

    # Not implemented yet
    pass


def update_google_drive(file_path: str) -> str|Exception:
    """Upload the file to Google Drive.
    
    Arguments:
        file_path {str} -- The path to the file to upload.

    Returns:
        str|Exception -- The file ID if the upload was successful, otherwise an
            Exception.
    """
    
    try:
        # Load service account credentials
        credentials = service_account.Credentials.from_service_account_file(
            './utils/service-credentials-google.json'
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
            media = MediaFileUpload(file_path)

            # Search for the file
            existing_files = search_file(service, filename, folder_id)
            if existing_files:
                # If the file exists, delete it
                for file in existing_files:
                    print(f"DELETING existing file: {file['name']} (ID: {file['id']})")
                    delete_file(service, file['id'])
            
            # Upload the file
            file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()
            print(f"New file uploaded id {file.get('id')}")

        # ### UPLOAD MAP ####
        # filename = 'unity_map.html',  # Name of the file to upload
        # folder_id = '1WTTQb7nKgOvnhLkt6EIQ6tlHEqf09luc'

        #  # File details
        # file_metadata = {
        #     'name': filename,  # Name of the file to upload
        #     'parents': [folder_id]   # ID of the folder to upload to
        # }
        # media = MediaFileUpload(filename)
        # # Upload the file
        # file = service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        return {'successfully uploaded to drive'}
        
    except Exception as e:
        print("Failed to upload to drive: ", e)
        return e

    # Only execute if file is run as main, not when imported by another module
if __name__ == "__main__":  # pragma: no cover
        
    API = os.getenv("FW_BMGF_KEY")
    fw = flywheel.Client(api_key=API)
       
    # Pass the Flywheel SDK client to "main".
    main(fw)