from unidecode import unidecode
import json
import re
import html
import sys

if len(sys.argv) < 2:
  exit("python clean_text.py <objectname>")

TagRemove = re.compile(r'<[^>]+>')

def RemoveTags(text):
    return TagRemove.sub("", text)

def CleanText(text):
    return unidecode(RemoveTags(html.unescape(text.strip())))

if __name__ == "__main__":
  alldata = json.load(open(sys.argv[1]))
  for key in alldata:
    alldata[key]['Text'] = CleanText(alldata[key]['Text'])
  json.dump(alldata, open(sys.argv[1], 'w'), indent=4)