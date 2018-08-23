import html
import json
import os
import re
import threading
from pprint import pprint
import configparser
import sys

from flask import Flask, flash, redirect, render_template, request
from flask_wtf import Form
from unidecode import unidecode
from wtforms import (FieldList, FormField, HiddenField, RadioField,
                     SelectField, StringField, SubmitField)

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

app = Flask(__name__)

SentimentOptions = ((-2, 'Very Negative'), (-1, 'Negative'), (0, 'Neutral'), (1, 'Positive'), (2, 'Very Positive'))

ResponseCount = 0

ProperData = json.load(open(TEXTFILE))

class Senitments(Form):
  SentimentForm = RadioField("Sentiment", choices=SentimentOptions)
  ID = HiddenField("ID")
  Type = HiddenField("Type")
  submit = SubmitField("Send")

# class SentimentList(Form):
#   AllSentiments = FieldList(FormField(Senitments), min_entries=2, max_entries=10)
  
app.config.update(dict(
    SECRET_KEY= config['KEYS']['SECRET_KEY'],
    WTF_CSRF_SECRET_KEY= config['KEYS']['CSRF_KEY']
))


@app.route('/<username>/<range>', methods=['get', 'post'])
def GetInput(username, range):
  global ResponseCount
  form = Senitments()

  LowRange = int(range.split("-")[0].strip())
  HighRange = int(range.split('-')[-1].strip())

  if request.method == 'POST':
    Response = request.form.to_dict()
    pprint(Response)
    #Response Type set manually in jinja2 to distinguish type of response. 
    if Response['Type'] == 'Text':
      ProperData[Response['ID']]['MarkedTextBy'].update({username: Response['SentimentForm']})
    #ProperData[Response['ID']]['MarkedBy'] = list(ProperData[Response['ID']]['MarkedBy'])
    elif Response['Type'] == 'ImageText':
      ProperData[Response['ID']]['MarkedTextImgBy'].update({username: Response['SentimentForm']})

    ResponseCount += 1
    if ResponseCount == ResponseBackup:
      #Overwrites original file. However, since the original dictionary is just being updated, no data is lost. 
      flash("Backing up responses.")
      WriteThread = threading.Thread(target=UpdateJSON, args=(ProperData, ))
      WriteThread.start()
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


if __name__ == "__main__":
  app.run(debug=True)
