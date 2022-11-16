import os, sys

path = os.path.abspath('.')
sys.path.append(path)
from controller.processing_data import *
from controller.get_data import *

df = get_vegan_day_data_from_db()
df = get_day_vegan_for_filter(df)
print(df.head())