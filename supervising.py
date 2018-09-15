import atexit
import json
import os
import re
import queue
import threading
from pprint import pprint
import configparser
import sys

from flask import Flask, flash, redirect, render_template, request, url_for
from flask_wtf import Form
import psycopg2
import psycopg2.extras
from unidecode import unidecode
from wtforms import (FieldList, FormField, HiddenField, RadioField,
                     SelectField, StringField, SubmitField, SelectMultipleField, widgets)

print("""
  ACCESS THE WEBSITE USING
  localhost:5000/<username>/
  example:
  localhost:5000/shikhar/
  """)

if len(sys.argv) < 2:
    exit("Usage:python supervising.py label_config.cfg")

config = configparser.ConfigParser()
config.read(sys.argv[1])


TEXTFILE = config['LOCATION']['TEXTFILE']
ALLADRECORD = config['LOCATION']['ALLADRECORD']

ResponseBackup = int(config['BACKUP']['RESPONSE'])

HOST = config['POSTGRES']['HOST']
DBNAME_LABELS = config['POSTGRES']['DBNAME_LABELS']
USER = config['POSTGRES']['USER']
PASSWORD = config['POSTGRES']['PASSWORD']
LabelsDBAuthorize = "host=%s dbname=%s user=%s password=%s" % (
    HOST, DBNAME_LABELS, USER, PASSWORD)
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

CategoryOptions = (('Donate', 'Donate'), ('Inform', 'Inform'), 
    ('Connect', 'Connect'), ('Move', 'Move'), ('Commercial', 'Commercial'))

ResponseCount = 0


MAINRUNNING = True
ThreadQueue = queue.Queue()

class MultiCheckBoxField(SelectMultipleField):
  widget = widgets.ListWidget(prefix_label=False)
  option_widget = widgets.CheckboxInput()

class Senitments(Form):
  TextSentimentForm = RadioField("TextSentiment", 
      choices=TextSentimentOptions)
  ImageTextSentimentForm = RadioField("ImageTextSentiment", 
      choices=ImageTextSentimentOpetions)
  CategoryForm = MultiCheckBoxField("Category", 
      choices=CategoryOptions)
  ID = HiddenField("ID")
  submit = SubmitField("Send")
  
app.config.update(dict(
    SECRET_KEY= config['KEYS']['SECRET_KEY'],
    WTF_CSRF_SECRET_KEY= config['KEYS']['CSRF_KEY']
))

label_type_DBdict = {}
labellers_DBdict = {}
categories_DBdict = {}
ProperData = json.load(open(TEXTFILE))
AllUserMarkedAds = json.load(open(ALLADRECORD))
AllAds = {k:ProperData[k] for k in ProperData.keys() if ProperData[k]["ImageURL"]}
SortedIDs = [k for k in sorted(list(AllAds.keys()))]





def InitializeDBVals():
  query = 'select * from label_values'
  cursor.execute(query)
  for row in cursor:
    label_type_DBdict[row['valuename']] = row['id']

  query = 'select * from users'
  cursor.execute(query)
  for row in cursor:
    labellers_DBdict[row['username']] = row['id']





@app.route('/<username>/', methods=['get', 'post'])
def RedirectFirstPage(username):
  global AllUserMarkedAds
  username=username.lower()
  if username not in AllUserMarkedAds:
    AllUserMarkedAds[username] = []
  AdMarkedCount = GetUserMakedCount(username)
  return redirect(url_for('GetInput', username=username, ID=SortedIDs[AdMarkedCount],
      AdMarkedCount=AdMarkedCount))





@app.route('/user?=<username>/ID?=<ID>/Count?=<AdMarkedCount>', methods=['get', 'post'])
def GetInput(username, ID, AdMarkedCount):
  form = Senitments()
  username=username.lower()

  if request.method == 'POST':
    Response = request.form.to_dict(flat=False)
    Response['ID'] = Response['ID'][0]
    Response['TextSentimentForm'] = Response['TextSentimentForm'][0]
    Response['ImageTextSentimentForm'] = Response['ImageTextSentimentForm'][0]
    pprint(Response)
    WriteToDB(Response, username)

    ProperData[Response['ID']]['MarkedTextBy'].update({username: Response['TextSentimentForm']})
    ProperData[Response['ID']]['MarkedTextImgBy'].update({username: Response['ImageTextSentimentForm']})
    ProperData[Response['ID']]['Category'].update({username: Response['CategoryForm']})
    AllUserMarkedAds[username].append(Response['ID'])
    AdMarkedCount = GetUserMakedCount(username)
    BackupData()
      
    return redirect(url_for('GetInput', username=username, ID=SortedIDs[AdMarkedCount],
        AdMarkedCount=AdMarkedCount))

  return render_template("sentimentanalysis.html", 
      AllData=AllAds, 
      ID = ID,
      Count = AdMarkedCount,
      AllAdsCount = len(ProperData),
      Form=form, 
      User=username)





def GetUserMakedCount(username):
  return len(AllUserMarkedAds[username])





def BackupData():
  BackupThread = threading.Thread(target=UpdateJSON)
  BackupThread.start()





def UpdateJSON():
  with open(ALLADRECORD, 'w') as f:
    json.dump(AllUserMarkedAds, f, indent=4)
  with open(TEXTFILE, 'w') as f:
    json.dump(ProperData, f, indent=4)





def WriteToDB(Response, Username):
  ad_id = int(Response['ID'])
  categories = Response['CategoryForm']
  TextSentiment = Response['TextSentimentForm']
  ImageTextSentiment = Response['ImageTextSentimentForm']
  SentimentQueryArgs = []

  if Username not in labellers_DBdict:
    labellers_DBdict[Username] = max(labellers_DBdict.values())+1
    InsertNewUserQuery = "insert into users (username) values (%s)"
    ThreadQueue.put((InsertNewUserQuery, (Username,)))
  
  user_id = labellers_DBdict[Username]
  TextSentimentID = label_type_DBdict[TextSentiment]
  ImageTextSentimentID = label_type_DBdict[ImageTextSentiment]
  CategoryIDs = [label_type_DBdict[category] for category in categories]
  ArgIDToInsert = [TextSentimentID, ImageTextSentimentID]
  ArgIDToInsert.extend(CategoryIDs)

  for IDToInsert in ArgIDToInsert:
    SentimentQueryArgs.extend([user_id, ad_id, IDToInsert]) 

  QueryHolders = ["(%s, %s, %s)"] * len(ArgIDToInsert)
  QueryHolders = ','.join(QueryHolders)

  InsertSentimentQuery = """
    INSERT into labels (user_id, ad_id, label_value_id)
    VALUES """ + QueryHolders

  ThreadQueue.put((InsertSentimentQuery, SentimentQueryArgs))

  InsertValueThread = threading.Thread(target=ThreadDBQuery, args=(ThreadQueue, ))
  InsertValueThread.start()





def ThreadDBQuery(ThreadQueue):
  while ThreadQueue:
    Query = ThreadQueue.get()
    InsertSentimentQuery, SentimentQueryArgs = Query[0], Query[1]
    print("working on: ", InsertSentimentQuery, SentimentQueryArgs)
    cursor.execute(InsertSentimentQuery, SentimentQueryArgs)
    #cursor.execute(Query)
    connection.commit()


#atexit.register(UpdateJSON)
atexit.register(connection.close)


if __name__ == "__main__":
  InitializeDBVals()
  app.run(host='0.0.0.0', port=5000)
