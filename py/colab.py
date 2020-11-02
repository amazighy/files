# HOW TO MOUNT THE DRIVE AND UNZIP FILES

from zipfile import ZipFile
file_name = file_path

with ZipFile(file_name, 'r') as zip:
    zip.extractall()
    print('Done')
