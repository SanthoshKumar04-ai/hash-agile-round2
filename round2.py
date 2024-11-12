from pymongo import MongoClient
import pandas as pd

client = MongoClient("mongodb://localhost:27017/")
db = client['employee_database']

def createCollection(p_collection_name):
    try:
        collection = db[p_collection_name]
        return collection
    except Exception as e:
        print(f"Error creating collection {p_collection_name}: {e}")
        return None

def indexData(p_collection_name, p_exclude_column):
    try:
        collection = db[p_collection_name]
        df = pd.read_csv("employee_data.csv")
        df_filtered = df.drop(columns=[p_exclude_column], errors='ignore')
        data = df_filtered.to_dict(orient='records')
        collection.insert_many(data)
    except Exception as e:
        print(f"Error indexing data into {p_collection_name}: {e}")

def searchByColumn(p_collection_name, p_column_name, p_column_value):
    try:
        collection = db[p_collection_name]
        query = {p_column_name: p_column_value}
        return list(collection.find(query))
    except Exception as e:
        print(f"Error searching in {p_collection_name}: {e}")
        return []

def getEmpCount(p_collection_name):
    try:
        collection = db[p_collection_name]
        return collection.count_documents({})
    except Exception as e:
        print(f"Error getting employee count in {p_collection_name}: {e}")
        return 0

def delEmpById(p_collection_name, p_employee_id):
    try:
        collection = db[p_collection_name]
        result = collection.delete_one({'EmployeeID': p_employee_id})
        return result.deleted_count > 0
    except Exception as e:
        print(f"Error deleting employee with ID {p_employee_id} in {p_collection_name}: {e}")
        return False

def getDepFacet(p_collection_name):
    try:
        collection = db[p_collection_name]
        pipeline = [
            {"$group": {"_id": "$Department", "count": {"$sum": 1}}}
        ]
        return list(collection.aggregate(pipeline))
    except Exception as e:
        print(f"Error getting department facets in {p_collection_name}: {e}")
        return []

if __name__ == "__main__":
    v_nameCollection = "Hash_JohnDoe"
    v_phoneCollection = "Hash_1234"

    createCollection(v_nameCollection)
    createCollection(v_phoneCollection)

    print(getEmpCount(v_nameCollection))

    indexData(v_nameCollection, 'Department')
    indexData(v_phoneCollection, 'Gender')

    print(getEmpCount(v_nameCollection))

    delEmpById(v_nameCollection, 'E02003')

    print(getEmpCount(v_nameCollection))

    print(searchByColumn(v_nameCollection, 'Department', 'IT'))
    print(searchByColumn(v_nameCollection, 'Gender', 'Male'))
    print(searchByColumn(v_phoneCollection, 'Department', 'IT'))

    print(getDepFacet(v_nameCollection))
    print(getDepFacet(v_phoneCollection))
