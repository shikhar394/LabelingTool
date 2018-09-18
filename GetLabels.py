import atexit
import json
import os
import queue
import threading
from pprint import pprint
import configparser
import sys

import psycopg2
import psycopg2.extras
from unidecode import unidecode
config = configparser.ConfigParser()
config.read(sys.argv[1])

HOST = config['POSTGRES']['HOST']
DBNAME_LABELS = config['POSTGRES']['DBNAME_LABELS']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
LabelsDBAuthorize = "host=%s dbname=%s user=%s password=%s" % (
    HOST, DBNAME_LABELS, USER, PASSWORD)
connection = psycopg2.connect(LabelsDBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor) 


def getUsers():
  Users = {}
  Query = "select * from users"
  cursor.execute(Query)
  for row in cursor:
    Users[row['id']] = row['username']
  return Users

def getLabels():
  Labels = {}
  LabelName = {}
  Query = "select l.user_id, lv.valuename, l.ad_id from label_values lv, labels l where l.label_value_id = lv.id"
  cursor.execute(Query)
  for row in cursor:
    Labels[row['user_id']] = {row['id'] : row['ad_id']}
    LabelName[row['id']] = row['valuename']
  return Labels, LabelName

if __name__ == '__main__':
  with open("Users.json") as f:
    json.dump(getUsers(), f, indent=4)
  Labels, LabelName = getLabels()
  with open("Labels.json") as f:
    json.dump(Labels, f, indent=4)
  with open("LabelName.json") as f:
    json.dump(LabelName, f, indent=4)