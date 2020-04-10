import sqlite3
from os.path import join
from clusterside.bootstrapper import parse_results


def test_parse_results(tmp_path):
    sqlite = sqlite3.connect(join(tmp_path, 'results.sqlite'))
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

    result = {
        'key-val': {'key1': 'val1', 'key2': 'val2'}
    }

    parse_results(result, tmp_path, 'fake_sample')

    c.execute("SELECT * FROM samples")
    assert c.fetchall() == [(1, 'fake_sample')]

    c.execute("SELECT * FROM key_val")
    assert c.fetchall() == [('key1', 'val1', 1),
                            ('key2', 'val2', 1)]

    c.close()
    sqlite.close()
