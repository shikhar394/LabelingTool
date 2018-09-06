import atexit
import html
import json
import os
import re
import queue
import threading
from pprint import pprint
import configparser
import sys

from flask import Flask, flash, redirect, render_template, request
from flask_wtf import Form
import psycopg2
import psycopg2.extras
from unidecode import unidecode
from wtforms import (FieldList, FormField, HiddenField, RadioField,
                     SelectField, StringField, SubmitField, SelectMultipleField)

print("""
  ACCESS THE WEBSITE USING
  localhost:5000/<username>/<range-of-ads>
  example:
  localhost:5000/shikhar/1-20
  """)

if len(sys.argv) < 2:
    exit("Usage:python supervising.py label_config.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])

#TODO Decide if per user web page and writes to dict with ID

TEXTFILE = config['LOCATION']['TEXTFILE']
ResponseBackup = int(config['BACKUP']['RESPONSE'])

HOST = config['POSTGRES']['HOST']
DBNAME = config['POSTGRES']['DBNAME']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
DBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME, USER, PASSWORD)
#connection = psycopg2.connect(DBAuthorize)
#cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

app = Flask(__name__)

SentimentOptions = ((-2, 'Very Negative'), (-1, 'Negative'), (0, 'Neutral'), (1, 'Positive'), (2, 'Very Positive'))

CategoryOptions = ((0, 'Donate'), (1, 'Inform'), (2, 'Connect'), (3, 'Move'))

ResponseCount = 0

ProperData = json.load(open(TEXTFILE))

MAINRUNNING = True
ThreadQueue = queue.Queue()

class Senitments(Form):
  SentimentForm = RadioField("Sentiment", choices=SentimentOptions)
  CategoryForm = SelectMultipleField("Category", choices=CategoryOptions)
  ID = HiddenField("ID")
  Type = HiddenField("Type")
  submit = SubmitField("Send")


# class DBInsertBackground(threading.Thread):
#   def __init__(self, interval=1):
#     threading.Thread.__init__(self)
#     self.interval = interval



#   def run(self):
#     try:
#       with SSHTunnelForwarder(
#         ('ccs1usr.engineering.nyu.edu', 22),
#         ssh_username='ss9131',
#         ssh_password='changemeNYU',
#         remote_bind_address=('localhost', 5432)) as server:

#         server.start()
#         print("Server connected")
#         connection = psycopg2.connect(DBAuthorize)
#         cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

#         while MAINRUNNING:
#           ResponseToInsert = ThreadQueue.get()
#           #TODO Write insertion script. 
#           ad_id = ResponseToInsert['ID']
#           sentiment_type = ResponseToInsert['Type']
#           type_id = label_type_DBdict[sentiment_type.lower()]
#           sentiment = ResponseToInsert['SentimentForm']
#           user_id = labllers_DBdict[Username.lower()]
#           print(ad_id, type_id, sentiment, user_id)

#     except:
#       print("Connection failed")
    
        

# class SentimentList(Form):
#   AllSentiments = FieldList(FormField(Senitments), min_entries=2, max_entries=10)
  
app.config.update(dict(
    SECRET_KEY= config['KEYS']['SECRET_KEY'],
    WTF_CSRF_SECRET_KEY= config['KEYS']['CSRF_KEY']
))

label_type_DBdict = {}
labllers_DBdict = {}

#InitializeDBVals()

@app.route('/<username>/<range>', methods=['get', 'post'])
def GetInput(username, range):
  global ResponseCount
  form = Senitments()

  LowRange = int(range.split("-")[0].strip())
  HighRange = int(range.split('-')[-1].strip())

  if request.method == 'POST':
    username=username.lower()
    Response = request.form.to_dict()
    pprint(Response)

    #Response Type set manually in jinja2 to distinguish type of response. 
    if Response['Type'] == 'Text':
      ProperData[Response['ID']]['MarkedTextBy'].update({username: Response['SentimentForm']})
    #ProperData[Response['ID']]['MarkedBy'] = list(ProperData[Response['ID']]['MarkedBy'])
    elif Response['Type'] == 'ImageText':
      ProperData[Response['ID']]['MarkedTextImgBy'].update({username: Response['SentimentForm']})
      if 'Category' not in ProperData[Response['ID']]: 
        ProperData[Response['ID']]['Category'] = {}
      ProperData[Response['ID']]['Category'].update({username: Response['CategoryForm']})


    ResponseCount += 1
    if ResponseCount == ResponseBackup:
      #Overwrites original file. However, since the original dictionary is just being updated, no data is lost. 
      flash("Backing up responses.")
      BackupThread = threading.Thread(target=UpdateJSON, args=(ProperData, ))
      BackupThread.start()
      ResponseCount = 0
      
    return redirect('/'+username+'/'+range)

  return render_template("sentimentanalysis.html", 
      AllData={k:ProperData[k] for k in list(ProperData.keys())[LowRange-1:HighRange]}, 
      Form=form, 
      User=username, 
      Range=range)





def UpdateJSON(Data):
  with open(TEXTFILE, 'w') as f:
    json.dump(Data, f, indent=4)





def WriteToDB(Response, Username):
  ad_id = Response['ID']
  sentiment_type = Response['Type']
  type_id = label_type_DBdict[sentiment_type.lower()]
  sentiment = Response['SentimentForm']
  user_id = labllers_DBdict[Username.lower()]
  print(ad_id, type_id, sentiment, user_id)




def InitializeDBVals():
  query = 'select * from label_type'
  cursor.execute(query)
  for row in cursor:
    label_type_DBdict[row['type']] = row['id']

  query = 'select * from labellers'
  cursor.execute(query)
  for row in cursor:
    labllers_DBdict[row['name']] = row['id']





atexit.register(UpdateJSON, Data=ProperData)


if __name__ == "__main__":
  app.run(debug=True)
