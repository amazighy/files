# HOW TO MOUNT THE DRIVE AND UNZIP FILES
https://colab.research.google.com/notebooks/snippets/drive.ipynb#scrollTo=P3KX0Sm0E2sF

from google.colab import files
from zipfile import ZipFile
from google.colab import drive
drive.mount('/content/gdrive')

uploaded = files.upload()
file_name = file_path

with ZipFile(file_name, 'r') as zip:
    zip.extractall()
    print('Done')
pip install openimages
 oi_download_dataset --base_dir ~/MLFinal/OpenImgs --labels Zebra Binoculars Woman Man Tree --format pascal --limit 30
