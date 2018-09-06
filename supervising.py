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
connection = psycopg2.connect(DBAuthorize)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)

app = Flask(__name__)

SentimentOptions = ((-2, 'Very Negative'), (-1, 'Negative'), (0, 'Neutral'), (1, 'Positive'), (2, 'Very Positive'))

CategoryOptions = (('Donate', 'Donate'), ('Inform', 'Inform'), ('Connect', 'Connect'), ('Move', 'Move'))

ResponseCount = 0

ProperData = json.load(open(TEXTFILE))

MAINRUNNING = True
ThreadQueue = queue.Queue()

class Senitments(Form):
  TextSentimentForm = RadioField("TextSentiment", choices=SentimentOptions)
  ImageTextSentimentForm = RadioField("ImageTextSentiment", choices=SentimentOptions)
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
  query = 'select * from label_type'
  cursor.execute(query)
  for row in cursor:
    label_type_DBdict[row['type']] = row['id']

  query = 'select * from labellers'
  cursor.execute(query)
  for row in cursor:
    labellers_DBdict[row['name']] = row['id']

  query = 'select * from ad_categories'
  cursor.execute(query)
  for row in cursor:
    categories_DBdict[row['category']] = row['id']





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
      AllData={k:ProperData[k] for k in list(ProperData.keys())[LowRange-1:HighRange]}, 
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
    InsertNewUserQuery = "insert into labellers (name) values (%s)" % (Username, )
    ThreadQueue.put(InsertNewUserQuery)
  
  ad_category_id = categories_DBdict[category]
  user_id = labellers_DBdict[Username]
  TextLabelID = label_type_DBdict['text']
  ImageTextLabelID = label_type_DBdict['text_image']

  InsertSentimentQuery = """
    INSERT into sentiments (ad_id, labeller_id, sentiment, data_type)
    VALUES (%s, %s, %s, %s), (%s, %s, %s, %s) """ % (
    ad_id, user_id, TextSentiment, TextLabelID, ad_id, user_id, ImageTextSentiment, ImageTextLabelID)

  print("Sentiment Query", InsertSentimentQuery)

  ThreadQueue.put(InsertSentimentQuery)

  InsertCategoryQuery = """
    INSERT into label_ad_categories (ad_id, labeller_id, category_id)
    VALUES (%s, %s, %s) """ % (ad_id, user_id, ad_category_id)
    
  ThreadQueue.put(InsertCategoryQuery)

  print("Category Query", InsertCategoryQuery)

  InsertValueThread = threading.Thread(target=ThreadDBQuery, args=(ThreadQueue, ))
  InsertValueThread.start()





def ThreadDBQuery(ThreadQueue):
  while ThreadQueue:
    Query = ThreadQueue.get()
    print("working on: ", Query)
    cursor.execute(Query)
    cursor.commit()





if __name__ == "__main__":
  atexit.register(UpdateJSON)
  InitializeDBVals()
  app.run(host='0.0.0.0', port=80)
