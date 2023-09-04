import  pymongo

#connecting the necessary database with the mongodb server

# obtain the connection string from the server to connect via python

client = pymongo.MongoClient("mongodb+srv://thineshkumar:<ThiKum10203040!>@practicecluster.kddvjwc.mongodb.net/?retryWrites=true&w=majority")
db=client['sample_weatherdata']
collection=db['data']