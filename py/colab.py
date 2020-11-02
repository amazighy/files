# HOW TO MOUNT THE DRIVE AND UNZIP FILES


from google.colab import files
from zipfile import ZipFile
from google.colab import drive
drive.mount('/content/gdrive')

uploaded = files.upload()
file_name = file_path

with ZipFile(file_name, 'r') as zip:
    zip.extractall()
    print('Done')
