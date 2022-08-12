import os
import re
import datetime
import shutil
import requests
import threading

from bs4 import BeautifulSoup
from typing import List
from pathlib import Path


DOWNLOADS_PATH = Path('downloads')
os.makedirs(DOWNLOADS_PATH, exist_ok=True)
os.makedirs(DOWNLOADS_PATH/'files', exist_ok=True)

def download_files(url:str, files:List[str], output_dir:Path, concurrency:int = 16):
    os.makedirs(output_dir, exist_ok=True)

    def _download(file):
        response = requests.get(f'{url}/{file}')
        assert response.ok, response

        with open(output_dir/file, 'wb') as fp:
            fp.write(response.content)

    threads = []
    for i, file in enumerate(files, start=1):
        print('downloading', file)
        t = threading.Thread(target=_download, args=[file])
        t.start()
        threads.append(t)

        if i % concurrency == 0:
            [t.join() for t in threads]
            threads = []

    [t.join() for t in threads]


def download_prefix_to_AS_mappings(date:datetime.date, days:int = 30):
    print('downloading prefix to AS')
    PREFIX_TO_AS_URL = 'https://publicdata.caida.org/datasets/routing/routeviews-prefix2as'
    DATE_FROM_FILE_RE = re.compile(r'routeviews-rv2-(\d{8})-\d+\.pfx2as\.gz')
    DOWNLOAD_PATH = DOWNLOADS_PATH/'rib-prefixes'
    
    start_date = date - datetime.timedelta(days=days + 1)
    months_seen = set()

    for i in range(days):
        d = start_date + datetime.timedelta(days=i)

        ym = d.strftime('%Y-%mm')
        if ym in months_seen:
            continue

        print('processing', ym)
        months_seen.add(ym)

        url = f'{PREFIX_TO_AS_URL}/{d.year}/{d.month:02d}'
        print('index:', url)

        response = requests.get(url)
        assert response.ok, response

        html = BeautifulSoup(response.text, features="html.parser")

        # get all the files from the <a> links in the HTML site that point to a .gz file
        files = [a.string for a in html.find_all('a') if a.string.endswith('.gz')]
        date_files = {
            DATE_FROM_FILE_RE.match(file).group(1): file
            for file in files
        }

        files_to_download = []
        for date, file in date_files.items():
            if date >= d.strftime('%Y%m%d'):
                files_to_download.append(file)

        download_files(url=url, files=files_to_download, output_dir=DOWNLOAD_PATH)
        extract_file(DOWNLOAD_PATH, DOWNLOADS_PATH/'files'/'rib.prefixes')



def download_relationships_and_cones(date:datetime.date, days=30):
    RELS_URL = 'https://publicdata.caida.org/datasets/as-relationships/serial-1'
    FILE_DATE_RE = re.compile(r'(\d{8})\.(ppdc-ases|as-rel).+')
    CONE_DOWNLOAD_PATH = DOWNLOADS_PATH/'cone-files'
    RELS_DOWNLOAD_PATH = DOWNLOADS_PATH/'rels-files'

    start_date, end_date = (date - datetime.timedelta(days=days+1)).strftime('%Y%m%d'), date.strftime('%Y%m%d')
    print('download relationships and cones from', start_date, 'to', end_date)

    response = requests.get(RELS_URL)
    assert response.ok, response

    def has_to_download(file:str):
        if not file.endswith('.bz2'):
            return False

        m = FILE_DATE_RE.match(file)
        if not m:
            return False

        file_date = m.group(1)

        return start_date <= file_date <= end_date

    html = BeautifulSoup(response.text, features="html.parser")
    files = [
        a.string
        for a in html.find_all('a')
        if has_to_download(a.string)
    ]

    download_files(url=RELS_URL, files=filter(lambda name: 'ppdc-ases' in name, files), output_dir=CONE_DOWNLOAD_PATH)
    download_files(url=RELS_URL, files=filter(lambda name: 'as-rel' in name, files), output_dir=RELS_DOWNLOAD_PATH)
    extract_file(CONE_DOWNLOAD_PATH, DOWNLOADS_PATH/'files'/'cone-file')
    extract_file(RELS_DOWNLOAD_PATH, DOWNLOADS_PATH/'files'/'rels-file')


def download_AS_orgs(date:datetime.date, days:int = 62):
    URL = 'https://publicdata.caida.org/datasets/as-organizations/'
    FILE_DATE_RE = re.compile(r'(\d{8})\.as-org2info\.txt\.gz')
    DOWNLOAD_PATH = DOWNLOADS_PATH/'as-org'

    start_date, end_date = (date - datetime.timedelta(days=days+1)).strftime('%Y%m%d'), date.strftime('%Y%m%d')
    print('download AS orgs from', start_date, 'to', end_date)

    response = requests.get(URL)
    assert response.ok, response

    def has_to_download(file:str):
        m = FILE_DATE_RE.match(file)
        if not m:
            return False

        file_date = m.group(1)

        return start_date <= file_date <= end_date

    html = BeautifulSoup(response.text, features="html.parser")
    files = [
        a.string
        for a in html.find_all('a')
        if has_to_download(a.string)
    ]

    download_files(url=URL, files=files, output_dir=DOWNLOAD_PATH)
    extract_file(DOWNLOAD_PATH, DOWNLOADS_PATH/'files'/'as2org')
    

def download_peering_db(date:datetime.date):
    print('download latest peering db available at date', date)
    
    # download latest available month
    URL = 'https://publicdata.caida.org/datasets/peeringdb-v2'

    response = requests.get(URL)
    assert response.ok, response

    html = BeautifulSoup(response.text, features="html.parser")
    years = [
        int(a.string.replace('/', ''))
        for a in html.find_all('a')
        if a.string.replace('/', '').isdigit()
    ]
    year = list(filter(lambda y: y <= date.year, sorted(years)))[-1]

    print(' - latest year is', year)

    response = requests.get(f'{URL}/{year}')
    assert response.ok, response

    html = BeautifulSoup(response.text, features="html.parser")
    months = [
        int(a.string.replace('/', ''))
        for a in html.find_all('a')
        if a.string.replace('/', '').isdigit()
    ]
    month = list(filter(lambda m: f'{year}-{m}' <= date.strftime('%Y-%m'), sorted(months)))[-1]
    print(' - latest month is', month)

    response = requests.get(f'{URL}/{year}/{month}')
    assert response.ok, response

    html = BeautifulSoup(response.text, features="html.parser")
    FILE_RE = re.compile(r'peeringdb_2_dump_.+\.json')
    files = [
        a.string
        for a in html.find_all('a')
        if FILE_RE.match(a.string)
    ]

    DOWNLOAD_PATH = DOWNLOADS_PATH/'peering_db'

    download_files(url=f'{URL}/{year}/{month}', files=files, output_dir=DOWNLOAD_PATH)
    shutil.move(DOWNLOAD_PATH/get_last_file(DOWNLOAD_PATH), DOWNLOADS_PATH/'files'/'peeringdb.json')
    

import bz2

def extract_bz2(input_file:str, output_file:Path):
    try:
        with bz2.open(input_file, "rb") as f:
        # Decompress data from file
            content = f.read()
        with open(output_file, 'wb+') as output:
            output.write(content)
    except: ValueError

import gzip

def extract_gz(input_file:str, output_file:Path):
    try:
        with gzip.open(input_file, "rb") as f:
        # Decompress data from file
            content = f.read()
        with open(output_file, 'wb+') as output:
            output.write(content)
    except: ValueError

def get_last_file(dir:Path):
    files = os.listdir(dir)
    files.sort()
    assert len(files) != 0
    return files[-1]

def extract_file(dir:Path, output_file:Path):
    last_file = get_last_file(dir)
    extension = last_file.split('.')[-1]
    if extension == 'bz2':
        extract_bz2(dir/last_file, output_file)
    elif extension == 'gz':
        extract_gz(dir/last_file, output_file)
    else:
         raise ValueError    

if __name__ == '__main__':
    date = datetime.date.today()

    download_prefix_to_AS_mappings(date=date)
    download_relationships_and_cones(date=date)
    download_AS_orgs(date=date)
    download_peering_db(date=date)