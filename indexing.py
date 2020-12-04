##This code is open-sourced software licensed under the MIT license
##Copyright  2020 Panos Kostakos, University of Oulu
##Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
##The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
##THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
##
##DISCLAIMER
##This code is used to crawl/parse data from gdeltproject project. By downloading this code, you agree to contact the corresponding data provider and verify you are allowed to use (including, but not limited, crawl/parse/download/store/process) all data obtained from the data source.


## Written by Panos Kostakos
## University of Oulu
## Nov. 2020
## The code crawls data from gdeltproject.com and sends requests to nominatim.openstreetmap.org.

import gzip
from datetime import datetime, timedelta
from time import sleep
import spacy
import urllib.parse
import requests, json, os, io
from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch, helpers
from textblob import TextBlob


client = Elasticsearch("localhost:9200")
nlp = spacy.load('en_core_web_sm')

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def getSentiment(text):
    parsedtext = TextBlob(text)
    sent=parsedtext.sentiment
    return {
    "sentiment":sent
    }

def getLocations(text):
    parsedDoc = nlp(text)
    ents =[]
    # See https://spacy.io/api/doc#ents
    for ent in  parsedDoc.ents:
        if  ent.label_ == "GPE":
            ents.append(ent)

    for i in ents:
        entValue = i.text.strip()
        url = "https://nominatim.openstreetmap.org/search?q="+str(entValue)+"&format=geocodejson&limit=1"
        res = requests.get(url)
        try:
            data = json.loads(res.content)
        except:
            print("there was some error with the json.loads")
        else:
            for i in data['features']:
                loc = (i["geometry"]['coordinates'])
                return loc


def getEntities(text):
#Methods and use of mapper-annotated-text can be found here: https://github.com/elastic/elasticsearch/blob/master/docs/plugins/mapper-annotated-text.asciidoc
    # Run the spacy model over the text
    parsedDoc = nlp(text)
    ents =[]
    # See https://spacy.io/api/doc#ents
    for ent in  parsedDoc.ents:
        if  ent.label_ != "ORDINAL" and  ent.label_ != "CARDINAL" and ent.label_!="DATE" \
                and ent.label_!="TIME" and ent.label_!="WORK  OF  ART" and ent.label_!="QUANTITY" :
            ents.append(ent)

    # offset = 0
    annotatedText = ""
    lastOffset=0

    entLabels = []

    for token in ents:

        entValue = token.text.strip()
        if len(entValue) > 1:
            offset = token.start_char
            if offset > lastOffset:
                annotatedText += text[lastOffset: offset]
            if  entValue not in entLabels:
                entLabels.append(entValue)
            # Add annotation to the text (annotations are entity type and label)
            annotatedText+="["+text[offset: token.end_char]+"]"+"("+ urllib.parse.quote(entValue)+"&"+urllib.parse.quote(token.label_) +")"
            offset = token.end_char
            lastOffset = offset
    if len(annotatedText) ==0:
        annotatedText =text
    else:
        if lastOffset < len(text):
            annotatedText += text[lastOffset: ]
    return {
        "entities": entLabels,
        "annotated_text": annotatedText
    }

# define a function that will load a text file
def get_data_from_text_file(self):
	return [l.strip() for l in open(str(self), encoding="utf8", errors='ignore')]


time = [dt.strftime('%Y%m%d%H%M%S') for dt in 
       datetime_range(datetime(2020, 1, 1, 0,3,0), datetime(2020, 8, 31, 0,0,0), 
       timedelta(minutes=1))]

for i in time:
	response = requests.get("http://data.gdeltproject.org/gdeltv3/gqg/"+str(i)+".gqg.json.gz")
	#while response.status_code == 200:
	gzip_file = io.BytesIO (response.content)
	with gzip.open (gzip_file, 'rt') as f:
		json_data = f.read ()
		with open(str(i), 'w+') as f:
			print(json_data, file=f)
			if os.stat(str(i)).st_size == 0:
				print(str(i)+ ' file was removed')
				os.remove(str(i))
				continue
			else: 
				docs = get_data_from_text_file(str(i))
				print ("String docs length:", len(docs))
				os.remove(str(i))

				doc_list = []
				for num, doc in enumerate(docs):
					try:
						doc = doc.replace("True", "true")
						doc = doc.replace("False", "false")
						dict_doc = json.loads(doc)
						dict_doc["timestamp"] = datetime.now()
						dict_doc["ners"] = getEntities(dict_doc["title"])
						#dict_doc["_id"] = num
						#dict_doc["GDLT_ID"] = num
						doc_list += [dict_doc]
					except json.decoder.JSONDecodeError as err:
						print ("ERROR for num:", num, "-- JSONDecodeError:", err, "for doc:", doc)
						print ("Dict docs length:", len(doc_list))
	doc_list2=[]    
	for i in doc_list:
	    for e in i["quotes"]:
	        x = {"date":i["date"], "url": i["url"], "title": i["title"], "lang": i["lang"], \
            "alltext": i["title"] + " " + e["pre"] + " " + e["quote"] +" "+ e["post"],\
            "sentiment": getSentiment(i["title"] + " " + e["pre"] + " " + e["quote"] +" "+ e["post"]),
	        "title_ner": i["ners"], "pre": e["pre"],"pre_ner": getEntities(e["pre"]), "gps": getLocations(e["quote"]),
	         "quote": e["quote"] ,"quote_ner": getEntities(e["quote"]), "post": e["post"],
	         "post_ner": getEntities(e["post"])}
	        doc_list2.append(x)

	try:
	    print ("\nAttempting to index the list of docs using helpers.bulk()")
	    resp = helpers.bulk(client,doc_list2,index = "panos_is_cool")
	    print ("helpers.bulk() RESPONSE:", resp)
	    print ("helpers.bulk() RESPONSE:", json.dumps(resp, indent=4))
	    
	except Exception as err:
	    print("Elasticsearch helpers.bulk() ERROR:", err)
