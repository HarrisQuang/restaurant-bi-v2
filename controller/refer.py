def write_record(name,details,engine):
    engine.execute("INSERT INTO records (name,details) VALUES ('%s','%s')" % (name,details))

def read_record(field,name,engine):
    result = engine.execute("SELECT %s FROM records WHERE name = '%s'" % (field,name))
    return result.first()[0]
    
def update_record(field,name,new_value,engine):
    engine.execute("UPDATE records SET %s = '%s' WHERE name = '%s'" % (field,new_value,name))

def write_dataset(name,dataset,engine):
    dataset.to_sql('%s' % (name),engine,index=False,if_exists='replace',chunksize=1000)

def read_dataset(name,engine):
    try:
        dataset = pd.read_sql_table(name,engine)
    except:
        dataset = pd.DataFrame([])
    return dataset

def list_datasets(engine):
    datasets = engine.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' ORDER BY table_name;")
    return datasets.fetchall()