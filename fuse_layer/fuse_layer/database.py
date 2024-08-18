from pymongo import MongoClient

# Connect to the local MongoDB server
client = MongoClient("mongodb://localhost:27017/")

# Access or create a database
db = client["mydatabase"]

# Access or create a collection (similar to a table in SQL)
collection = db["people"]


def put_item():
    # Example objects
    person1 = {"age": 30, "gender": "male", "nationality": "USA"}
    person2 = {"age": 25, "gender": "female", "nationality": "Canada"}

    # Insert the objects into the collection
    collection.insert_one(person1)
    collection.insert_one(person2)

    # Ensure unique index on the combination of fields
    collection.create_index([("age", 1), ("gender", 1), ("nationality", 1)], unique=True)


def get_item():
    # Find one document that matches the query
    person1 = collection.find_one({"age": 30, "gender": "male", "nationality": "USA"})
    print(person1)
    person2 = collection.find_one({"age": 25, "gender": "female", "nationality": "Canada"})
    print(person2)


if __name__ == "__main__":
    get_item()
