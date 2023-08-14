import os
import pandas as pd 
import shutil
import zipfile
import pathlib

#from os import listdirs


class FaresExtractor:

    error_list = []

    def __init__(self, fares_data_folder):

        self.fares_data_folder = fares_data_folder

    def xml_extract(self):

        for subdir, dirs, files in os.walk(self.fares_data_folder):
            for file in files:
                filepath = subdir + os.sep + file
                if filepath.endswith(".zip"):
                    zip_ref = zipfile.ZipFile(filepath) # create zipfile object
                    zip_ref.extractall(subdir) # extract file to dir
                    zip_ref.close() # close file
                    os.remove(filepath) # delete zipped file
    
    def listdirs(self, rootdir):

        list_of_subdir = []

        for file in os.listdir(rootdir):
            d = os.path.join(rootdir, file)
            for file2 in os.listdir(d):
                e = os.path.join(d, file2)
                if os.path.isdir(e):
                    list_of_subdir.append(e)

        return list_of_subdir
    
    def tree_clean(self):

        sublist = FaresExtractor.listdirs(self, self.fares_data_folder)

        for i in sublist:
            path = pathlib.Path(str(i))
            dest = path.parent
            for filename in os.listdir(path):
                new_src = str(path) + os.sep + str(filename)
                new_dst = str(dest)
                print(new_dst)
                if new_src.endswith(".xml"):
                    shutil.move(new_src, new_dst)
        
        for i in sublist:
            os.rmdir(i)

    

    