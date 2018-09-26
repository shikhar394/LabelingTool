import configparser
import datetime
import json
import os
import requests
import sys
import time
from collections import namedtuple


import psycopg2
import psycopg2.extras

if len(sys.argv) < 2:
    exit("Usage:python3 import_ads_to_db.py import_ads_to_db.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)






if __name__ == "__main__":
  Query = """
  select archive_id, text, image_url 
  from ads 
  where 
  image_url is not null
  and
  page_id not in (
    select p.id 
    from 
    ads a, pages p 
    where 
    a.page_id=p.id 
    and a.id in 
    (468135543637922, 177018779651263, 140208533414388, 1672278109547815, 197288211122592,93982622643, 1452020118232532, 1895632567401776, 2135252976548992, 395440887616881, 1777754642278928, 213250312722466, 1189736404501911))
  order by random() 
  limit 2000;
  """
  cursor.execute(Query)
  ID_Text = {}
  for (ArchiveID, text, image_url) in cursor.fetchall():
    if image_url.strip() != '' and text.strip() != '':
      if requests.get(image_url).status_code == 200:
        ID_Text[int(ArchiveID)] = {
          'Text': text,
          'ImageURL': image_url,
          'MarkedTextBy': {},
          'MarkedTextImgBy': {},
          'Category': {}
        }
      if len(ID_Text) > 500:
        break
    
  with open("TextSample.json", 'w') as f:
    json.dump(ID_Text, f, indent=4)
