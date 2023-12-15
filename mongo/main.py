from dotenv import load_dotenv,find_dotenv
from pymongo import MongoClient
import os
import pprint

load_dotenv(find_dotenv())

password=os.environ.get("mongodbpwd")

connection_string=f"mongodb+srv://johntripleceid:{password}@cluster0.xcosrek.mongodb.net/?retryWrites=true&w=majority"
client=MongoClient(connection_string)

dbs=client.list_database_names()
test_db=client.test
collections=test_db.list_collection_names()
#print(collections)

def insert_test_doc():
    collections=test_db.test
    test_document= {
        "name" : "john",
        "type" : "test"
    }
    inserted_id=collections.insert_one(test_document).inserted_id
# print(inserted_id)

insert_test_doc()


production=client.production
person_collection=production.person_collection

def create_document():
    first_names=["john","jim","tim","chris","thanos","andrew"]
    last_names=['Big',"Daddy","Sion","Great","Wall","FF15"]
    age=[20,21,22,23,24,25]

    docs=[]

    for first_name,last_name,age in zip(first_names,last_names,age):
        doc={"first_name": first_name, "last_name" : last_name, "age" : age}
        docs.append(doc)

    person_collection.insert_many(docs)

#create_document()

printer=pprint.PrettyPrinter()

def find_all_people():
    people=person_collection.find()
    for person in people:
        printer.pprint(person)

find_all_people()

def find_andrew():
    andrew=person_collection.find_one({"first_name" : "andrew"})
    printer.pprint(andrew)

find_andrew()

def count_all_people():
    count=person_collection.count_documents(filter={})
    print("Number of people ", count)

count_all_people()

def get_person_by_id(person_id):
    from bson.objectid import ObjectId

    _id=ObjectId(person_id)
    person=person_collection.find_one({"_id" : _id})
    printer.pprint(person)

get_person_by_id("655dda5a3c3a2dadf76878ed")

def get_age_range(min_age,max_age):
    query={ "$and": [
                {"age" : {"$gte" : min_age}},
                {"age" : {"$lte" : max_age}}
            ]
    }
    people=person_collection.find(query).sort("age")
    for person in people:
        printer.pprint(person)

get_age_range(22,23)

def project_collumns():
    collumns={"first_name" :1 , "last_name" : 1, "_id" : 0}
    people=person_collection.find({},collumns)
    for person in people:
        printer.pprint(person)

project_collumns()

#def update_person(person_id):
#   from bson.objectid import ObjectId

#  _id=ObjectId(person_id)

#  all_updates={
#     "$set" : {"new" : True},
#      "$rename" : {"first_name" : "first", "last_name" : "last"},
#     "$inc" : {"age" : 1}
#  }
#   person_collection.update_one({"_id" : _id}, all_updates)

# person_collection.update_one({"_id" : _id}, {"$unset" : {"new" : ""}})

# update_person("655dda5a3c3a2dadf76878ed")

def delete_doc_by_id(person_id):
    from bson.objectid import ObjectId
    _id=ObjectId(person_id)
    person_collection.delete_one({"_id" : _id})

delete_doc_by_id("655e4c94b441d4650182c9ce")

adress={
    "_id": "655e4c94b441d4650182c9dg",
    "street": "Pavlou Mela",
    "number": 12,
    "city" : "athens"
}

def add_adress_embed(person_id,adress):
    from bson.objectid import ObjectId
    _id=ObjectId(person_id)

    person_collection.update_one({"_id" : _id}, {"$addToSet" : {"adresses" : adress}})

add_adress_embed("655e4c94b441d4650182c9c9",adress)

