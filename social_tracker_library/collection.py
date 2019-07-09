import json
import redis

def collection_keywords_list(title, ownerId):
    """ list the keywords of a given collection """
    db_m = __demoConnection()

    keys = db_m.Collection.find_one({'title': title,
                                     'ownerId': ownerId})["keywords"]

    keys_l = [k['keyword'].lower() for k in keys]

    return keys_l


def collection_add_keywords(title, ownerId, new_keywords):
    """ add new keywords in a given collection """

    db_m = __demoConnection()

    keys = db_m.Collection.find_one({'title': title,
                                     'ownerId': ownerId})["keywords"]

    # create a set in order to not include a duplicate keyword
    set_current_keys = set()
    for k in keys:
        set_current_keys.add(k["keyword"].lower())

    set_new_keys = set()
    for k in new_keywords:
        if k.lower() not in set_current_keys:
            set_new_keys.add(k.lower())

    for new_key in set_new_keys:
        keys.append({'keyword': new_key})

    db_m.Collection.update_one({'title': title, 'ownerId': ownerId},
                               {'$set': {'keywords': keys}})

    edited_collection = db_m.Collection.find_one({'title': title,
                                                  'ownerId': ownerId})

    with open(__CONF_FILE, 'r') as fp:
        conf = json.load(fp)

    r = redis.StrictRedis(host=conf["redis"]["host"],
                          port=conf["redis"]["port"], db=conf["redis"]["db"])
    r.publish("collections:edit", json.dumps(edited_collection))


def collection_remove_keywords(title, ownerId, keywords):
    """ remove keywords in a given collection """

    db_m = __demoConnection()

    keys = db_m.Collection.find_one({'title': title,
                                     'ownerId': ownerId})["keywords"]

    # lower the keywords which should be removed
    for i in range(len(keywords)):
        keywords[i] = keywords[i].lower()

    # turn the list into set - to improve performance (theoretically)
    keywords = set(keywords)

    # create a set in order to not include a duplicate keyword
    new_keys = list()
    for k in keys:
        if k["keyword"].lower() not in keywords:
            new_keys.append({'keyword': k["keyword"].lower()})

    db_m.Collection.update_one({'title': title, 'ownerId': ownerId},
                               {'$set': {'keywords': new_keys}})

    edited_collection = db_m.Collection.find_one({'title': title,
                                                  'ownerId': ownerId})

    with open(__CONF_FILE, 'r') as fp:
        conf = json.load(fp)

    r = redis.StrictRedis(host=conf["redis"]["host"],
                          port=conf["redis"]["port"], db=conf["redis"]["db"])
    r.publish("collections:edit", json.dumps(edited_collection))


def list_collections():
    """ lists current collections in the system """

    print("List of the collections in the system")
    print()

    db_m = __demoConnection()

    collection_settings = db_m.Collection.find()

    for col in collection_settings:
        print("title:", str(col["title"]))
        print("owner:", str(col["ownerId"]))
        print("keywords:", str(col["keywords"]))
        print("accounts:", str(col["accounts"]))
        print("status:", str(col["status"]))
        print("locations:", str(col["nearLocations"]))


def create_collection(title, ownerId, keywords):
    """ create a collection """

    # future implementation
    # users = []  Users (e.g. prefeituraunicamp facebook, jornaloglobo twitter)
    # location = []  Location

    creationDate = int(datetime.datetime.now().strftime("%s")) * 1000

    _id = str(math.floor(random.random() * 90000) + 10000) + ownerId + str(
        creationDate)

    since = int((datetime.datetime.now() - datetime.timedelta(
        days=15)).strftime("%s")) * 1000

    keywords_strconf = []

    # set(keywords) in order to avoid duplicates
    set_keywords = set()
    for k in keywords:
        set_keywords.add(k.lower())

    for k in set_keywords:
        keywords_strconf.append({"keyword": k})

    db_m = __demoConnection()

    new_collection = {
        "_id": _id,
        "title": title,
        "ownerId": ownerId,
        "keywords": keywords_strconf,
        "accounts": [],
        "creationDate": creationDate,
        "updateDate": creationDate,
        "since": since,
        "status": "running",
        "nearLocations": [],
    }

    db_m.Collection.insert_one(new_collection)

    with open(__CONF_FILE, 'r') as fp:
        conf = json.load(fp)

    r = redis.StrictRedis(host=conf["redis"]["host"],
                          port=conf["redis"]["port"], db=conf["redis"]["db"])
    r.publish("collections:new", json.dumps(new_collection))
