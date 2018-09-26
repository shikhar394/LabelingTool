import json
import os
import csv
from pprint import pprint 

Labels = json.load(open(os.path.join('AdRecords', 'NewLabels.json')))
Users = json.load(open(os.path.join('AdRecords', 'Users.json')))
LabelName = json.load(open(os.path.join('AdRecords', "LabelName.json")))
LabelerID = ['1', '3', '6']
CategoryNamesList = {11: "Donate", 12: "Inform", 13: "Move", 14: "Connect",
    15: "Commercial", 16: "Not Political"}





def SelectUserLabels():
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
  Restructures the label file as Ad_id: {User_id: sentiment}
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
  




def CategorizeSentiment(Text):
  """
  Decides sentiment for a given ad. 
  Checks for 2/3 or more agreement on a sentiment. Calls SettleClearMajority().
  If a clear majority is not meant, it looks for a "soft majority". Calls SettleSoftMajority()
  """
  for ID in Text:
    SentimentScore = {}
    for labelers in Text[ID].keys():
      Sentiment = Text[ID][labelers]
      if Sentiment not in SentimentScore:
        SentimentScore[Sentiment] = 0
      SentimentScore[Sentiment] += 1
    
    if len(SentimentScore) < 3:
      Text[ID]['ClearMajority'] = SettleClearMajority(SentimentScore)
    else:
      Sentiment = SettleSoftMajority(SentimentScore)
      if Sentiment != -1000:
        Text[ID]['SoftMajority'] = Sentiment
      else:
        Text[ID]['NoClearMajority'] = Sentiment
  return Text





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
  LabelsToCheck = SelectUserLabels()
  Text, Text_Image, Categories = CategorizeLabels(LabelsToCheck)
  SentimentFieldnames = ['ID', 'damon', 'ratan', 'shikhar', "ClearMajority", "SoftMajority", "NoClearMajority"]
  WriteCSV(CategorizeSentiment(Text), "Text", SentimentFieldnames)
  WriteCSV(CategorizeSentiment(Text_Image), "ImageText", SentimentFieldnames)
  CategoryFieldnames = ['ID', 'damon', 'ratan', 'shikhar', 'Category']
  WriteCSV(ClassifyCategory(Categories), 'Categories', CategoryFieldnames)
