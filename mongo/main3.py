from dotenv import load_dotenv,find_dotenv
import os
import pprint
from pymongo import MongoClient
import json
from pprint import PrettyPrinter
load_dotenv(find_dotenv())

password=os.environ.get("mongodbpwd")

connection_string=f"mongodb+srv://johntripleceid:{password}@cluster0.xcosrek.mongodb.net/?retryWrites=true&w=majority"
client=MongoClient(connection_string)
printer=PrettyPrinter()
jeapordy_db=client.jeapordy_db
question= jeapordy_db.jeapordy

# fuzzy matching

def fuzzy_matching(word):
    result = question.aggregate([
        {
            "$search" : {
                "index" : "language_search",
                "text" : {
                    "path" : "category",
                    "query" : word
                }
            }
        }
    ])
    printer.pprint(list(result))

#fuzzy_matching("computer")

#synonym matching

def fuzzy_matching_synonyms(word):
    result = question.aggregate([
        {
            "$search" : {
                "index" : "language_search",
                "text" : {
                    "path" : "category",
                    "query" : word,
                    "synonyms": "mapping"
                }
            }
        }
    ])
    printer.pprint(list(result))

#fuzzy_matching_synonyms("laptop")

def autocomplete(word):
    result=question.aggregate([
        {
            "$search" : {
                "index" : "language_search", 
                "autocomplete" : {
                    "query" : word,
                    "path" : "question",
                    "tokenOrder" : "sequential",
                    "fuzzy": {}
                }
            }
        },
        {
            "$project" : {
                "_id" : 0,
                "question" : 1
            }
        }
    ])
    printer.pprint(list(result))

autocomplete("footbal star")

#compound queries