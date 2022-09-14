from sqlalchemy import create_engine
import pandas as pd
name = 'Bo'
details = '24 chui'
engine = create_engine("postgresql://postgres:12345678@localhost:5432/demo_db")
engine.execute("CREATE TABLE IF NOT EXISTS records (name text PRIMARY KEY, details text)")
engine.execute("INSERT INTO records (name,details) VALUES ('%s','%s')" % (name,details))

name = 'new-dataset'
ds = pd.DataFrame({'Name': ['Hai', 'Heo', 'Tin', 'Bo'], 'Age': [28, 18, 22, 28]})
ds.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)