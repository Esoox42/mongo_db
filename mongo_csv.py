import pprint
uri = 'mongodb://ue8eadbecr19tspjxpml:MMexDIJFCb2bW80bZzog@n1-c2-mongodb-clevercloud-customers.services.clever-cloud.com:27017,n2-c2-mongodb-clevercloud-customers.services.clever-cloud.com:27017/bt3af6edkiqnoj8?replicaSet=rs0'
client = pymongo.MongoClient(uri)

db = client['bt3af6edkiqnoj8']
client.list_database_names()

collection = db["high_speed_train_statistics"]

with open('/content/gdrive/MyDrive/STATS502/project_1/SNCF_Monthly_Regularity_TGV.csv', newline='') as csvfile:
    reader = csv.DictReader(csvfile, delimiter=';', quotechar='"')

    next(reader)  # skip the header row
    for row in reader:
        # Extract the values and convert to the specified data types
        doc = {
            '_id': str(row['_id']),
            'Year': int(row['Year']),
            'Month': int(row['Month']),
            'Type': str(row['Type']),
            'Departure station': str(row['Departure station']),
            'Arrival station': str(row['Arrival station']),
            'Average travel time': int(row['Average travel time']),
            'Number of expected travels': int(row['Number of expected travels']),
            'Number of cancelled trains': int(row['Number of cancelled trains']),
            'Number of late trains at departure': int(row['Number of late trains at departure']),
            'Average delay at departure of all trains': float(row['Average delay at departure of all trains'].replace(',', '.')),
            'Number of late trains at arrival': int(row['Number of late trains at arrival']),
            'Average delay at arrival of all trains': float(row['Average delay at arrival of all trains'].replace(',', '.')),
            'Comment on delay at arrival': row['Comment on delay at arrival'],
            'Number of late trains > 15min': int(row['Number of late trains > 15min']),
            'Average delay of late trains > 15min': float(row['Average delay of late trains > 15min'].replace(',', '.')),
            'Number of late trains > 30min': int(row['Number of late trains > 30min']),
            'Number of late trains > 60min': int(row['Number of late trains > 60min']),
            'Percentage of late trains due to external causes': float(row['Percentage of late trains due to external causes'].replace(',', '.')),
            'Percentage of late trains due to infrastructure': float(row['Percentage of late trains due to infrastructure'].replace(',', '.')),
            'Percentage of late trains due to traffic management': float(row['Percentage of late trains due to traffic management'].replace(',', '.')),
            'Percentage of late trains due to rolling stock': float(row['Percentage of late trains due to rolling stock'].replace(',', '.')),
            'Percentage of late trains due to station management and reuse of material': float(row['Percentage of late trains due to station management and reuse of material'].replace(',', '.')),
            'Percentage of late trains due to passenger traffic': float(row['Percentage of late trains due to passenger traffic'].replace(',', '.'))
        }

        # Insert the document into the collection
        collection.insert_one(doc)

pp = pprint.PrettyPrinter(depth=6)
