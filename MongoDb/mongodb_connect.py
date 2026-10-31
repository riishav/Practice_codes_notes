from pymongo import MongoClient

# MongoDB connection string
conn_string = "mongodb+srv://<username>:<password>@mongo-db-cluster-new.6wvma1w.mongodb.net/?retryWrites=true&w=majority&appName=mongo-db-cluster-new"

# Connect to MongoDB
client = MongoClient(conn_string)

# Select the database
db = client['airbnb_mart']

# Select the collection
collection = db['airbnb_property_reviews']

# Insert a document
# insert_result = collection.insert_one({'name': 'John Doe', 'age': 30})
# print(f"Document inserted with id: {insert_result.inserted_id}")

# Query the collection
 ## it will read as Property type is Apartment and Room type is Private room
find_query = {'property_type' : 'Apartment', 'room_type': 'Private room'}  

## it will read as Property type is House or Apartment
find_query = { '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ] }  

## it will read as Room type is Private room and Property type is House or Apartment
find_query = { 'room_type': 'Private room', 
               '$or' : [ {'property_type' : 'House'} , {'property_type' : 'Apartment'} ]  
            }

## it will read as Accommodates is greater than 2
find_query = { 'accommodates': { '$gt' : 2} }

count_results = collection.count_documents(find_query)
print("Total Documents Found : ",count_results)

# Print documents fetched from find query
find_results = collection.find(find_query)
for doc in find_results:
    print(doc)




#   _id in the $group -- this id is not the mongodb default it,rather the id of the group, which is the country in this case
# grp1 = [
#     {
#         "$group": {                                     -- this will group by country and then calculate the average price for each country
#             "_id": "$address.country",  # Field to group by 
#             "avg_price": {"$avg": "$price"}  # Field to avg
#         }
#     },
#     {
#         "$project": {             -- this will project the fields we want to see in the output ( country and avg_proce in this case)
#             "country": "$_id",    -- country will be the value of the _id field from the group stage which is "$address.country" here
#             "avg_price": {"$toDouble": "$avg_price"},
#             "_id": 0   -- this will remove the _id field from the output, as we don't need it in the final output ( 1 means include the field, 0                           means exclude the field)
#         }
#     }
# ]

grp2 = [
    {
        "$group": {
            "_id": {
                "country": "$address.country",
                "city": "$address.suburb"
            },
            "avg_price": {"$avg": "$price"}
        }
    },
    {
        "$project": {
            "country": "$_id.country",
            "city": "$_id.city",
            "avg_price": {"$toDouble": "$avg_price"},
            "_id": 0       #this will remove the _id field from the output, as we don't need it in the final output ( 1 means include the field, 
                          #0 means exclude the field)
        }
    }
]


results = collection.aggregate(grp2)
for result in results:
    print(result)


# Close the connection
client.close()