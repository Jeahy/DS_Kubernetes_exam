from pymongo import MongoClient
from pprint import pprint
import re

client = MongoClient(
    host="127.0.0.1",
    port = 27017
)

#1(b)
list_dbs = client.list_database_names()

#1(c)
sample = client["sample"]
list_clts = sample.list_collection_names()

#1(d)
books = sample["books"]
one_doc = books.find_one({})

#1(e)
nr_docs = books.count_documents({})

#2(a)
nr_books1 = books.count_documents({"pageCount":{"$gte": 400}})
nr_books2 = books.count_documents({
    "$and":[
        {"pageCount":{"$gte": 400}},
        {"status":{"$eq":"PUBLISH"}}
        ]})

#2(b)
regex_pattern = re.compile("Android")
nr_books3 = books.count_documents({
    "$or":[ 
        {"shortDescription": regex_pattern},
        {"shortDescription": regex_pattern}
        ]})

#2(c)
dist_cat = books.aggregate([
    {"$group": {
            "_id": None,
            "cat1": {"$addToSet": {"$arrayElemAt": ["$categories", 0]}},
            "cat2": {"$addToSet": {"$arrayElemAt": ["$categories", 1]}}
        }}])

#2(d)
nr_books4 = books.count_documents({
    "$or":[ 
        {"longDescription": re.compile("Pathon")},
        {"longDescription": re.compile("Java")},
        {"longDescription": re.compile("C/+/+")},
        {"longDescription": re.compile("Scala")}
        ]})

#2(e)
cat_stats = books.aggregate([
    {"$unwind": "$categories"},
    {"$group": {
            "_id": "$categories",
            "maxPages": { "$max": "$pageCount" },
            "minPages": { "$min": "$pageCount" }, 
            "avgPages": { "$avg": "$pageCount" }
        }}])

#2(f)
pub_date = books.aggregate(
    [
        {"$project": {"year": {"$year": "$publishedDate"}, 
                      "month": {"$month": "$publishedDate"}, 
                      "day": {"$dayOfMonth": "$publishedDate"}}},
        {"$match": {"year": {"$gt": 2009 }}},
        {"$limit":20}
    ]
)

#2(g)
authors = books.aggregate(
    [{
        "$unwind": "$authors" 
    },
    {
        "$group": {
            "_id": "$_id",
            "title": {"$first": "$title"},
            "authors": {"$push": "$authors"}, 
            "numAuthors": {"$sum": 1}}
    },
    {   "$project": {
            "_id": 1,
            "title": 1,
            "authors": 1,
            "numAuthors": 1}
    },
    { "$sort": {"publishedDate": -1} },
    { "$limit": 20 } 
    ]
)

#PRINT RESULTS INTO REX.TXT

filepath = "/home/ubuntu/DS_repo_vm/res.txt"

with open(filepath, "w") as file: 
    file.write(f"1(b) List of available databases:\n")
    for b in list_dbs:
        file.write(b+'\n')

    file.write(f"\n1(c) List of available collections:\n")
    for c in list_clts:
        file.write(c+'\n')

    file.write(f"\n1(d) One document of the books collection:\n")
    pprint(one_doc, stream=file)

    file.write(f"\n1(e) Number of documents in the books collection:\n{nr_docs}\n")

    file.write(f"\n2(a) Number of books with more than 400 pages in the books collection:\n{nr_books1}\n")

    file.write(f"\n2(a) Number of published books with more than 400 pages in the books collection:\n{nr_books2}\n")

    file.write(f"\n2(b) Number of books with keyword 'Android' in description:\n{nr_books3}\n")

    file.write(f"\n2(c) Category list 1:\n") 
    dist_cat_list = list(dist_cat)
    if dist_cat_list and 'cat1' in dist_cat_list[0]:
        for c in dist_cat_list[0]['cat1']:
            file.write(c + '\n')

    file.write(f"\n2(c) Category list 2:\n")
    if dist_cat_list and 'cat2' in dist_cat_list[0]:
        for i in dist_cat_list[0]['cat2']:
            file.write(i + '\n')
   

    file.write(f"\n2(d) Number of books about Python, Java, C++ or Scala:\n{nr_books4}\n")

    file.write(f"\n2(e) Category stats:\n")
    for e in cat_stats:
        file.write(f"Category: {e['_id']}\n")
        file.write(f"  Maximum Pages: {e['maxPages']}\n")
        file.write(f"  Minimum Pages: {e['minPages']}\n")
        file.write(f"  Average Pages: {e['avgPages']}\n\n")

    file.write(f"\n2(f) 20 Books published after 2009:\n")
    for f in pub_date:
        file.write(f"{f}\n")

    file.write(f"\n2(g) Books listed with individual authors:\n")
    for doc in list(authors): 
        for e in range(doc['numAuthors']):
            file.write(f"author{e + 1}: {doc['authors'][e]}\n")
        
