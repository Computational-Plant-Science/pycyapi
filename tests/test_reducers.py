import sqlite3
import os
from os.path import join
from clusterside.executors import reduce_csv, reduce_files

def create_db(path):
    sqlite = sqlite3.connect(path)
    c = sqlite.cursor()
    c.execute('''
              CREATE TABLE samples(
                id INTEGER PRIMARY KEY,
                name TEXT
              );
    ''')
    c.execute('''
              CREATE TABLE files(
                file_path TEXT,
                sample INTEGER,
                FOREIGN KEY(sample) REFERENCES samples(id)
              );
    ''')
    c.execute('''
              CREATE TABLE key_val(
                key TEXT,
                data TEXT,
                sample INTEGER,
                FOREIGN KEY(sample) REFERENCES samples(id)
              );
    ''')
    sqlite.commit()

    return c

def add_sample(c,name):
    c.execute("INSERT INTO samples (name) VALUES (?)",(name,))
    return c.execute('SELECT last_insert_rowid()').fetchone()[0]

def test_reduce_csv(tmp_path):
    c = create_db(join(tmp_path,'results.sqlite'))

    def add_results(sample_name,data):
        sample_id = add_sample(c,sample_name)
        for key,val in data.items():
            c.execute('INSERT INTO key_val (key,data,sample) VALUES (?,?,?)',(key,val,sample_id))

    add_results('sample1',{'key1': 'val1', 'key2':'val2'})
    add_results('sample2',{'key1': 'val3', 'key2':'val4'})

    reduce_csv(c,join(tmp_path,'results.csv'))

    with open(join(tmp_path,'results.csv'),'r') as infile:
        res = infile.read()
        print(res)
        assert res == 'sample,key1,key2\nsample1,val1,val2\nsample2,val3,val4\n'

def test_reduce_csv_missing_keys(tmp_path):
    c = create_db(join(tmp_path,'results.sqlite'))

    def add_results(sample_name,data):
        sample_id = add_sample(c,sample_name)
        for key,val in data.items():
            c.execute('INSERT INTO key_val (key,data,sample) VALUES (?,?,?)',(key,val,sample_id))

    add_results('sample1',{'key1': 'val1', 'key2':'val2'})
    add_results('sample2',{'key1': 'val3'})

    reduce_csv(c,join(tmp_path,'results.csv'))

    with open(join(tmp_path,'results.csv'),'r') as infile:
        res = infile.read()
        print(res)
        assert res == 'sample,key1,key2\nsample1,val1,val2\nsample2,val3,NULL\n'

def test_reduce_file(tmp_path):
    c = create_db(join(tmp_path,'results.sqlite'))

    def add_files(sample_name, files):
        sample_id = add_sample(c,sample_name)

        #Create a fake sample file and put the fake results files in it
        os.mkdir(join(tmp_path,sample_name))
        for file in files:
            with open(join(tmp_path,sample_name,file),'w') as fout:
                fout.write(file)
            c.execute('INSERT INTO files (file_path,sample) VALUES (?,?)',(file,sample_id))

    add_files('sample1',['file1_path', 'file2_path'])
    add_files('sample2',['file1_path', 'file2_path'])

    reduce_files(c,tmp_path,tmp_path)

    file_list = ['sample1_file1_path', 'sample1_file2_path',
                 'sample2_file1_path', 'sample2_file2_path']

    files = list(os.listdir(join(tmp_path,'files')))
    assert set(files) == set(file_list)

def test_no_files(tmp_path):
    c = create_db(join(tmp_path,'results.sqlite'))


    c.execute("INSERT INTO samples (name) VALUES (?)",('sample1',))


    assert not reduce_files(c,tmp_path,tmp_path)

def test_no_key_val(tmp_path):
    sqlite = sqlite3.connect(join(tmp_path,'results.sqlite'))
    c = sqlite.cursor()
    c.execute('''
              CREATE TABLE samples(
                id INTEGER PRIMARY KEY,
                name TEXT
              );
    ''')
    c.execute('''
              CREATE TABLE files(
                file_path TEXT,
                sample INTEGER,
                FOREIGN KEY(sample) REFERENCES samples(id)
              );
    ''')
    c.execute('''
              CREATE TABLE key_val(
                key TEXT,
                data TEXT,
                sample INTEGER,
                FOREIGN KEY(sample) REFERENCES samples(id)
              );
    ''')
    sqlite.commit()


    c.execute("INSERT INTO samples (name) VALUES (?)",('sample1',))

    assert not reduce_csv(c,join(tmp_path,'results.csv'))
