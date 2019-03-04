import sqlite3
import os
from os.path import join
from clusterside.executors import reduce_csv, reduce_files

def test_reduce_csv(tmp_path):
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

    result = {'key1': 'val1', 'key2':'val2'}

    for key,val in result.items():
        c.execute('INSERT INTO key_val (key,data,sample) VALUES (?,?,?)',(key,val,1))

    reduce_csv(c,join(tmp_path,'results.csv'))

    with open(join(tmp_path,'results.csv'),'r') as infile:
        res = infile.read()
        assert res == 'sample,key1,key2\nsample1,val1,val2\n'

def test_reduce_file(tmp_path):
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

    result = ['file1_path', 'file2_path']

    #Create a fake sample file and put the fake results files in it
    os.mkdir(join(tmp_path,'sample1'))
    for file in result:
        with open(join(tmp_path,'sample1',file),'w') as fout:
            fout.write(file)

    for file in result:
        c.execute('INSERT INTO files (file_path,sample) VALUES (?,?)',(file,1))

    reduce_files(c,tmp_path,tmp_path)

    for file in os.listdir(join(tmp_path,'files')):
        assert file in ["%s_%s"%('sample1',r) for r in result]
