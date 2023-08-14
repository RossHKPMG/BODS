import requests
import shutil
import os
from urllib.request import urlopen
from shutil import copyfileobj

data = requests.get(
    "https://data.bus-data.dft.gov.uk/api/v1/fares/dataset?noc=ROSS&status=published&limit=1&offset=0&api_key=ab70c6d46880d6fb3418a656e5c8e40fb5aa8f2c"
)
response_data = data.json()
print(response_data)

name_id = response_data["results"][0]["id"]
data_download_folder = f"{str(name_id)}.zip"
operator_name = response_data["results"][0]["operatorName"]
url = response_data["results"][0]["url"]

parent_dir = "C:/Users/Ross/Documents/BODS/"
path = os.path.join(parent_dir, operator_name)
os.mkdir(path)

with urlopen(url) as in_stream, open(zip, "wb") as out_file:
    copyfileobj(in_stream, out_file)

filepath = os.path.join(parent_dir, zip)
shutil.move(filepath, path)
