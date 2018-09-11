import atexit
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
DBNAME_LABELS = config['POSTGRES']['DBNAME_LABELS']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
LabelsDBAuthorize = "host=%s dbname=%s user=%s password=%s" % (HOST, DBNAME_LABELS, USER, PASSWORD)
connection = psycopg2.connect(LabelsDBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor) 

print("Connected to DB")
app = Flask(__name__)

TextSentimentOptions = (('Very Negative Text', 'Very Negative'), ('Negative Text', 'Negative'), 
    ('Neutral Text', 'Neutral'), ('Positive Text', 'Positive'), 
    ('Very Positive Text', 'Very Positive'))

ImageTextSentimentOpetions = (('Very Negative ImageText', 'Very Negative'), 
    ('Negative ImageText', 'Negative'), ('Neutral ImageText', 'Neutral'), 
    ('Positive ImageText', 'Positive'), ('Very Positive ImageText', 'Very Positive'))

CategoryOptions = (('Donate', 'Donate'), ('Inform', 'Inform'), ('Connect', 'Connect'), ('Move', 'Move'), ('Commercial', 'Commercial'))

ResponseCount = 0

ProperData = json.load(open(TEXTFILE))

MAINRUNNING = True
ThreadQueue = queue.Queue()

class Senitments(Form):
  TextSentimentForm = RadioField("TextSentiment", choices=TextSentimentOptions)
  ImageTextSentimentForm = RadioField("ImageTextSentiment", choices=ImageTextSentimentOpetions)
  CategoryForm = RadioField("Category", choices=CategoryOptions)
  ID = HiddenField("ID")
  submit = SubmitField("Send")
  
app.config.update(dict(
    SECRET_KEY= config['KEYS']['SECRET_KEY'],
    WTF_CSRF_SECRET_KEY= config['KEYS']['CSRF_KEY']
))

label_type_DBdict = {}
labellers_DBdict = {}
categories_DBdict = {}





def InitializeDBVals():
  query = 'select * from label_values'
  cursor.execute(query)
  for row in cursor:
    label_type_DBdict[row['valuename']] = row['id']

  query = 'select * from users'
  cursor.execute(query)
  for row in cursor:
    labellers_DBdict[row['username']] = row['id']





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
    WriteToDB(Response, username)

    ProperData[Response['ID']]['MarkedTextBy'].update({username: Response['TextSentimentForm']})
    ProperData[Response['ID']]['MarkedTextImgBy'].update({username: Response['ImageTextSentimentForm']})
    ProperData[Response['ID']]['Category'].update({username: Response['CategoryForm']})

    ResponseCount += 1
    if ResponseCount == ResponseBackup:
      #Overwrites original file. However, since the original dictionary is just being updated, no data is lost. 
      BackupData()
      ResponseCount=0
      
    return redirect('/'+username+'/'+range)

  return render_template("sentimentanalysis.html", 
      AllData={k:ProperData[k] for k in list(ProperData.keys())[LowRange-1:HighRange] if ProperData[k]['ImageURL']}, 
      Form=form, 
      User=username, 
      Range=range)





def BackupData():
  BackupThread = threading.Thread(target=UpdateJSON)
  BackupThread.start()




def UpdateJSON():
  with open(TEXTFILE, 'w') as f:
    json.dump(ProperData, f, indent=4)





def WriteToDB(Response, Username):
  ad_id = Response['ID']
  category = Response['CategoryForm']
  TextSentiment = Response['TextSentimentForm']
  ImageTextSentiment = Response['ImageTextSentimentForm']

  if Username not in labellers_DBdict:
    labellers_DBdict[Username] = max(labellers_DBdict.values())+1
    InsertNewUserQuery = "insert into users (username) values ('%s')" % (Username, )
    ThreadQueue.put(InsertNewUserQuery)
  
  user_id = labellers_DBdict[Username]
  TextSentimentID = label_type_DBdict[TextSentiment]
  ImageTextSentimentID = label_type_DBdict[ImageTextSentiment]
  CategoryID = label_type_DBdict[category]

  print(TextSentimentID, ImageTextSentimentID, CategoryID)

  InsertSentimentQuery = """
    INSERT into labels (user_id, ad_id, label_value_id)
    VALUES (%s, %s, %s), (%s, %s, %s), (%s, %s, %s)""" % (
    user_id, ad_id, TextSentimentID, user_id, ad_id, ImageTextSentimentID,
    user_id, ad_id, CategoryID)

  print("Sentiment Query", InsertSentimentQuery)

  ThreadQueue.put(InsertSentimentQuery)

  InsertValueThread = threading.Thread(target=ThreadDBQuery, args=(ThreadQueue, ))
  InsertValueThread.start()





def ThreadDBQuery(ThreadQueue):
  while ThreadQueue:
    Query = ThreadQueue.get()
    print("working on: ", Query)
    cursor.execute(Query)
    connection.commit()


atexit.register(UpdateJSON)
atexit.register(connection.close)


if __name__ == "__main__":
  InitializeDBVals()
  app.run(host='127.0.0.1', port=5000)
