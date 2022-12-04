import os, sys

path = os.path.abspath('./controller')
sys.path.append(path)
from processing_data import *
from get_data import *

df = get_vegan_day_data()
print(df)