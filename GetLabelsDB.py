import atexit
import json
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
  Query = "select l.user_id, lv.valuename, lv.id, l.ad_id from label_values lv, labels l where l.label_value_id = lv.id"
  cursor.execute(Query)
  for row in cursor:
    if row['user_id'] in Labels:
      Labels[row['user_id']].append({row['id'] : row['ad_id']})
    else:
      Labels[row['user_id']] = []
    LabelName[row['id']] = row['valuename']
  return Labels, LabelName

if __name__ == '__main__':
  with open("Users.json", 'w') as f:
    json.dump(getUsers(), f, indent=4)
  Labels, LabelName = getLabels()
  with open("Labels.json", 'w') as f:
    json.dump(Labels, f, indent=4)
  with open("LabelName.json", 'w') as f:
    json.dump(LabelName, f, indent=4)
