import requests
import shutil
import os
from urllib.request import urlopen
from shutil import copyfileobj
from zipfile import ZipFile
from io import BytesIO

'''
Currently gets the API request depending on the information that you provide, (NOC, limit, status, etc.) Bods Compliant
will hopefully be added in as another field to search by soon.

Once you got your response from the api, it will run through each request, grab the information such as ID/name for folder
creation, then grab the download URL.

It will then create a folder based on the operators name, if the folder as not been craeted yet, it will create a new one,
otherwise if it exists, it will simply keep adding in the information to the spcififed folder

'''

data = requests.get(
    "https://data.bus-data.dft.gov.uk/api/v1/fares/dataset?noc=NIBS&status=published&limit=10&offset=0&api_key=ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c"
)
response_data = data.json()

for key in range(0, (len(response_data) + 1)):
    name_id = response_data["results"][key]["id"]
    data_download_folder = f"{str(name_id)}.zip"
    operator_name = response_data["results"][0]["operatorName"]
    url = response_data["results"][key]["url"]

    parent_dir = "C:/Users/rosshamilton/OneDrive - KPMG/Documents/BODS Project/BODS/"
    path = os.path.join(parent_dir, operator_name)
    if os.path.exists(path):
        with urlopen(url) as in_stream:
            with ZipFile(BytesIO(in_stream.read())) as zfile:
                zfile.extractall(path)
    else:
        os.mkdir(path)
        with urlopen(url) as in_stream:
            with ZipFile(BytesIO(in_stream.read())) as zfile:
                zfile.extractall(path)

