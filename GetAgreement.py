import atexit
import configparser
import csv
import json
import os
import sys
from pprint import pprint

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

LabelerID = ['1', '3', '6']
CategoryNamesList = {11: "Donate", 12: "Inform", 13: "Move", 14: "Connect",
    15: "Commercial", 16: "Not Political"}





def SelectUserLabels(Labels):
  """
  Weeds out the labelers we don't need to see the labels from. 
  """
  LabelsToCheck = {}
  for user in Labels.keys():
    if user in LabelerID:
      LabelsToCheck[Users[user]] = [{int(label.keys()[0]):int(label.values()[0])} for label in Labels[user]]
  return LabelsToCheck




  
def CategorizeLabels(LabelsToCheck):
  """
  Reads through the labels.json file. 
  Normalizes and categorizes the sentiments based on their label_id value.
  Restructures the label file as Ad_id: {User_id: sentiment} to allow for majority agreement. 
  """
  Text = {}
  Text_Image = {}
  Categories = {}
  for user in LabelsToCheck:
    for labels in LabelsToCheck[user]:
      ID = labels.values()[0]
      Label = labels.keys()[0]
      if Label < 6:
        Label -= 3 # Normalize sentiments on a -2, -1, 0, 1, 2 scale
        if ID not in Text:
          Text[ID] = {}
        Text[ID][user] = Label
      elif Label > 5 and Label < 11:
        Label -= 8 # Normalize sentiments on a -2, -1, 0, 1, 2 scale
        if ID not in Text_Image:
          Text_Image[ID] = {}
        Text_Image[ID][user] = Label
      else:
        if ID not in Categories:
          Categories[ID] = {}
        if user not in Categories[ID]:
          Categories[ID][user] = []
        Categories[ID][user].append(Label)
  return Text, Text_Image, Categories
  




def CategorizeSentiment(Payload):
  """
  Decides sentiment for a given ad. 
  Checks for 2/3 or more agreement on a sentiment. Calls SettleClearMajority().
  If a clear majority is not meant, it looks for a "soft majority". Calls SettleSoftMajority()
  """
  for ID in Payload:
    SentimentScore = {}
    for labelers in Payload[ID].keys():
      Sentiment = Payload[ID][labelers]
      if Sentiment not in SentimentScore:
        SentimentScore[Sentiment] = 0
      SentimentScore[Sentiment] += 1
    
    if len(SentimentScore) < 3:
      Payload[ID]['ClearMajority'] = SettleClearMajority(SentimentScore)
    else:
      Sentiment = SettleSoftMajority(SentimentScore)
      if Sentiment != -1000:
        Payload[ID]['SoftMajority'] = Sentiment
      else:
        Payload[ID]['NoClearMajority'] = Sentiment
  return Payload





def SettleClearMajority(SentimentScore):
  """ Looks for 2/3 or more majority agreement. """
  maxScore = 0
  maxSentiment = 0
  for sentiment, score in SentimentScore.items():
    if maxScore < score:
      maxScore = score
      maxSentiment = sentiment
  return maxSentiment





def SettleSoftMajority(SentimentScore):
  """
  If clear majority is not achieved, extreme sentiments are normalized as regular ones 
  (-2 -> -1 | 2 -> 1) and then looks for clear majority. 
  Returns -1000 if a soft majority is not reached either.
  """
  SoftSentiment = {-1:0, 0:0, 1:0}
  for sentiment in SentimentScore:
    if sentiment == -1 or sentiment == -2:
      SoftSentiment[-1] += 1 
    elif sentiment == 0:
      SoftSentiment[0] += 1
    else:
      SoftSentiment[1] += 1
  for sentiment, score in SoftSentiment.items():
    if score > 1:
      return sentiment
  return -1000





def ClassifyCategory(Categories):
  """
  Settles majority categories labeled on the ads.
  """
  AdCategories = {}
  for ID in Categories:
    Labels = Categories[ID]
    for labeler in Labels:
      CategoriesSelected = Labels[labeler]
      for i in range(len(CategoriesSelected)):
        CategoryName = CategoryNamesList[CategoriesSelected[i]]
        CategoriesSelected[i] = CategoryNamesList[CategoriesSelected[i]]
        if CategoryName not in AdCategories:
          AdCategories[CategoryName] = 0
        AdCategories[CategoryName] +=1 
      Labels[labeler] = ','.join(Labels[labeler])
    FinalCategories = ','.join([Category for Category in AdCategories if AdCategories[Category] > 1])
    Categories[ID]['Category'] = FinalCategories
    AdCategories = {}
  return Categories





def getUsers():
  """
  Gets all the labelers and their IDs from the DB and stores it in AdRecords/Users.json
  """
  Users = {}
  Query = "select * from users"
  cursor.execute(Query)
  for row in cursor:
    Users[row['id']] = row['username']
  with open(os.path.join('AdRecords', 'Users.json'), 'w') as f:
    json.dump(Users, f, indent=4)
  return Users 





def getLabels():
  """
  Gets all the labelname and ID of labels from DB and stores it in AdRecords/LabelName.json.
  Gets all the label_id, user_id, ad_id from db and stores it in AdRecords/Label.json.
  """
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

  with open(os.path.join('AdRecords', 'Labels.json'), 'w') as f:
    json.dump(Labels, f, indent=4)

  with open(os.path.join('AdRecords', 'LabelName.json'), 'w') as f:
    json.dump(LabelName, f, indent=4)

  return LabelName, Labels





def WriteCSV(Payload, Type, FieldNames):
  with open(Type+'.csv', 'w') as f:
    fieldname = FieldNames
    writer = csv.DictWriter(f, fieldnames=fieldname, restval='-')
    writer.writeheader()
    for ID in Payload:
      Row = {'ID': ID}
      Row.update(Payload[ID])
      pprint(Row)
      writer.writerow(Row)
    




if __name__ == "__main__":
  Users = getUsers()
  LabelName, Labels = getLabels()
  LabelsToCheck = SelectUserLabels(Labels)
  Text, Text_Image, Categories = CategorizeLabels(LabelsToCheck)

  SentimentFieldnames = ['ID', 'damon', 'ratan', 'shikhar', "ClearMajority", "SoftMajority", "NoClearMajority"]
  WriteCSV(CategorizeSentiment(Text), "Text", SentimentFieldnames)
  WriteCSV(CategorizeSentiment(Text_Image), "ImageText", SentimentFieldnames)
  CategoryFieldnames = ['ID', 'damon', 'ratan', 'shikhar', 'Category']
  WriteCSV(ClassifyCategory(Categories), 'Categories', CategoryFieldnames)
