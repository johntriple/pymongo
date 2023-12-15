from dotenv import load_dotenv,find_dotenv
from pymongo import MongoClient
import os
import pprint
from datetime import datetime as dt

load_dotenv(find_dotenv())

password=os.environ.get("mongodbpwd")

connection_string=f"mongodb+srv://johntripleceid:{password}@cluster0.xcosrek.mongodb.net/?retryWrites=true&w=majority&authSource=admin"
client=MongoClient(connection_string)

dbs=client.list_database_names()
production=client.production

printer=pprint.PrettyPrinter()

def create_book_collection():
    book_validator= {
        "$jsonSchema" : {
            "bsonType" : "object",
            "required" : ["title",'authors',"publish_date","type","copies"],
            "properties" : {
                "title" : {
                    "bsonType" : "string",
                    "description" : "The title of the book and is requiered"
                },
                "authors" :{
                    "bsonType" : "array",
                    "items" : {
                        "bsonType" : "objectId",
                        "description" : "must be an object Id and is required"
                    }
                },
                "publish_date" : {
                    "bsonType" : "date",
                    "description" : "must be a date and is required"
                },
                "type" : {
                    "enum" : ["Fiction", "Non-Fiction"],
                    "description" : "can only be one of the specified values and is requiered"
                },
                "copies" : {
                    "bsonType" : "int",
                    "minimum" : 0,
                    "description" : "must be an integer greater than 0 and is required"
                },
            }
        }
    }
    try:
        production.create_collection("book")
    except Exception as e:
        print(e)
    production.command("collMod","book",validator=book_validator)


create_book_collection()

def create_author_collection():
    author_validator={
        "$jsonSchema": {
            "bsonType" : "object",
            "required" : ['first_name',"last_name","date_of_birth"],
            "properties":{
                "first_name" : {
                    "bsonType" : "string",
                    "description" : "must be string and is required"
                },
                "last_name" : {
                    "bsonType" : "string",
                    "description" : "must be string and is required"
                },
                "date_of_birth" :{
                    "bsonType" : "date",
                    "description" : "must be date and is required"
                },
            }
        }
    }
    try:
        production.create_collection("author")
    except Exception as e:
        print(e)
    production.command("collMod","author",validator=author_validator)

create_author_collection()

def create_data():
    authors=[
        {
            "first_name" :"john",
            "last_name" : "trip",
            "date_of_birth" : dt(1997,7,11)
        },
        {
            "first_name" :"jim",
            "last_name" : "triple",
            "date_of_birth" : dt(1999,2,15)
        },
        {
            "first_name" :"jack",
            "last_name" : "daw",
            "date_of_birth" : dt(1992,1,11)
        },
        {
            "first_name" :"alexander",
            "last_name" : "triplovich",
            "date_of_birth" : dt(2000,12,19)
        },
        {   "first_name" :"philip",
            "last_name" : "papias",
            "date_of_birth" : dt(2004,2,18)
        },
    ]
    author_collection=production.author
    authors=author_collection.insert_many(authors).inserted_ids

    books=[
        {
            'title' : "Big Daddy Sion",
            "authors" : [authors[0]],
            "publish_date" : dt.today(),
            "type" : 'Fiction',
            'copies' : 1000
        },
        {
            'title' : "Ceid",
            "authors" : [authors[0]],
            "publish_date" : dt(2022,12,15),
            "type" : 'Non-Fiction',
            'copies' : 25
        },
        {
            'title' : "Miss Fortune",
            "authors" : [authors[2]],
            "publish_date" : dt(2011,1,19),
            "type" : 'Fiction',
            'copies' : 365
        },
        {
            'title' : "Ragnarok",
            "authors" : [authors[4],authors[3]],
            "publish_date" : dt(2019,10,27),
            "type" : 'Fiction',
            'copies' : 250000
        },
        {
            'title' : "FF15",
            "authors" : [authors[1]],
            "publish_date" : dt.today(),
            "type" : 'Non-Fiction',
            'copies' : 15
        },
    ]

    book_collection = production.book
    book_collection.insert_many(books)

create_data()

books_containing_Sion=production.book.find({"title" : {"$regex" : "Sion{1}"}})
#printer.pprint(list(books_containing_Sion))

#authors_and_books=production.author.aggregate([{
#   "$lookup" : {
#        "from" : "book",
#        "localField" : "_id",
#       "foreignField" : "authors",
#        "as" : "books"
#    }
#}])

#printer.pprint(list(authors_and_books))

authors_book_count=production.author.aggregate([
    {
        "$lookup" : {
            "from" : "book",
            "localField" : "_id",
            "foreignField" : "authors",
            "as" : "books"
        }
    },
    {
        "$addFields" : {
            "total_books" : {"$size" : "$books"}
        }
    },
    {
        "$project" : {"first_name" : 1, "last_name" : 1, "total_books" : 1, "_id" : 0},
    }
])

printer.pprint(list(authors_book_count))


#import pyarrow
#from pymongoarrow.api import Schema 
#from pymongoarrow.monkey import patch_all
#import pymongoarrow as pma

#patch_all()