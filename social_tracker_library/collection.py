import json
import redis
import datetime
import math
import random
import os
import time
import re
from pymongo import MongoClient

from .constants import CONF_FILE, IMAGEDIR, VIDEODIR, URLDIR, HEAD_ITEMS
from .constants import HEAD_MEDIA, HEAD_SET_URLS, HEAD_SET_LINKS, HEAD_LINK_LIST
from .constants import CSVIMAGE, CSVITEMS, CSVLINKS, CSVVIDEO, CSVSETURL
from .constants import CSVSETLINKS
from .utils import Date, Text, OSUtils, CSVUtils

class collection:
    def __init__(self, title, ownerId, start_date=None, end_date=None, original=True):
        self.title = title
        self.ownerId = ownerId
        self.start_date = start_date
        self.end_date = end_date
        self.original = orginal


    def __demoConnection(self):
        """ returns database connection """

        with open(CONF_FILE, 'r') as fp:
            conf = json.load(fp)

        client = MongoClient(
            conf["mongo"]["path"],
            username=conf["mongo"]["username"],
            password=conf["mongo"]["password"],
            authSource=conf["mongo"]["authsource"],
            unicode_decode_error_handler='ignore')

        # returning database
        return client.Demo


    @classmethod
    def __mediaItem(cls, m_id):
        """ returns mediaItem given id """

        db_m = cls.__demoConnection()

        return db_m.MediaItem.find({"_id": m_id})[0]


    def __publish_redis(self, channel, message):
        """ publish changes in redis (in order to inform listeners) """

        with open(CONF_FILE, 'r') as fp:
            conf = json.load(fp)

        r = redis.StrictRedis(host=conf["redis"]["host"],
                              port=conf["redis"]["port"], db=conf["redis"]["db"])

        r.publish(channel, message)


    def keywords_list(self):
        """ list the keywords of a given collection """
        db_m = self.__demoConnection()

        keys = db_m.Collection.find_one({'title': self.title,
                                         'ownerId': self.ownerId})["keywords"]

        keys_l = [k['keyword'].lower() for k in keys]

        return keys_l


    def add_keywords(self, new_keywords):
        """ add new keywords in a given collection """

        if isinstance(new_keywords, str):
            raise Exception("Error - please pass the keywords as a list")

        db_m = self.__demoConnection()

        keys = db_m.Collection.find_one({'title': self.title,
                                         'ownerId': self.ownerId})["keywords"]

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

        self.__publish_redis("collections:edit", json.dumps(edited_collection))


    def exists(self):
        """ verify if a collection exists """
        db_m = self.__demoConnection()

        col = db_m.Collection.find_one({'title': self.title,
                                         'ownerId': self.ownerId})

        return bool(col)


    def remove_keywords(self, keywords):
        """ remove keywords in a given collection """

        if isinstance(keywords, str):
            raise Exception("Error - please pass the keywords as a list")

        db_m = self.__demoConnection()

        keys = db_m.Collection.find_one({'title': self.title,
                                         'ownerId': self.ownerId})["keywords"]

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

        db_m.Collection.update_one({'title': self.title, 'ownerId': self.ownerId},
                                   {'$set': {'keywords': new_keys}})

        edited_collection = db_m.Collection.find_one({'title': self.title,
                                                      'ownerId': self.ownerId})

        self.__publish_redis("collections:edit", json.dumps(edited_collection))

''
    def list_collections(cls):
        """ lists current collections in the system """

        print("List of the collections in the system")
        print()

        db_m = cls.__demoConnection()

        collection_settings = db_m.Collection.find()

        for col in collection_settings:
            print("title:", str(col["title"]))
            print("owner:", str(col["ownerId"]))
            print("keywords:", str(col["keywords"]))
            print("accounts:", str(col["accounts"]))
            print("status:", str(col["status"]))
            print("locations:", str(col["nearLocations"]))
            print("")


    def item_count(self):
        """ count the qtde of items given a collection """

        # connection with Mongo
        db_m = self.__demoConnection()

        item_query = self.__item_query(start_date, end_date)

        return db_m.Item.find(item_query).count()


    def create(self, keywords):
        """ create a collection """

        # future implementation
        # users = []  Users (e.g. prefeituraunicamp facebook, jornaloglobo twitter)
        # location = []  Location

        if self.exists():
            raise Exception("Error - Collection with same title and owner already exists")

        creationDate = Date.now()

        _id = str(math.floor(random.random() * 90000) + 10000) + self.ownerId + str(
            creationDate)

        since = Date.now(-15)

        keywords_strconf = []

        # set(keywords) in order to avoid duplicates
        set_keywords = set()
        for k in keywords:
            set_keywords.add(k.lower())

        for k in set_keywords:
            keywords_strconf.append({"keyword": k})

        db_m = self.__demoConnection()

        new_collection = {
            "_id": _id,
            "title": self.title,
            "ownerId": self.ownerId,
            "keywords": keywords_strconf,
            "accounts": [],
            "creationDate": creationDate,
            "updateDate": creationDate,
            "since": since,
            "status": "running",
            "nearLocations": [],
        }

        db_m.Collection.insert_one(new_collection)

        self.__publish_redis("collections:new", json.dumps(new_collection))


    def __collection_regstr_query(keywords):
        """ receives a set of keywords and returns the related regex query """

        if len(keywords) == 0:
            return ""

        regstr = "("
        for key in keywords:
            for word in key["keyword"].split(" "):
                    regstr = regstr + "(?=.*" + word + ")"
            regstr = regstr + "|"

        regstr = regstr[:-1] + ")"

        return re.compile(regstr, re.IGNORECASE)


    def __item_query(self):
        """ given the name of a collection, its owner and start/end date, returns
            the query to pass as parameter of db.Collection.Find() """

        db_m = self.__demoConnection()

        # get the register concerning the collection passed as parameter
        collection_settings = db_m.Collection.find({
                                                "ownerId": ownerId,
                                                "title": title
                                                 })[0]

        # generate the query
        items_query = dict()

        items_query["title"] = __collection_regstr_query(
            collection_settings["keywords"])

        # if original is true, query should return only orginal. If orginal is
        # false, query should ignore whether item is original or not
        if original is True:
            items_query["original"] = original

        if start_date is not None:
            items_query["publicationTime"] = {"$gte": utils.date2tmiles(start_date)}
        else:
            items_query["publicationTime"] = {"$gte": collection_settings["since"]}

        if end_date is not None:
            items_query["publicationTime"] = {"$lte": utils.date2tmiles(end_date)}

        return items_query


    def ContentGenerator(self):
        """ generator of the content of a given collection """

        # connection with Mongo
        db_m = self.__demoConnection()

        item_query = self.__item_query()

        items = db_m.Item.find(item_query)

        for it in items:
            yield it


    def tags_facet_query(self, variant_analysis=True):
        """ facet query in 'tags' field of Item collection. """

        db_m = self.__demoConnection()

        item_query = self.__item_query(start_date, end_date, original)

        facet_query = db_m.Item.aggregate([
            {'$match': item_query},
            {'$unwind': "$tags"},
            {'$group': {"_id": "$tags", "count": {'$sum': 1}}},
            {'$sort': {'count': -1}},
            {'$group': {"_id": None, "tags_facet": {'$push': {"tags": "$_id",
                                                              "count": "$count"
                                                              }}}},
            {'$project': {"_id": 0, "tags_facet": 1}}
        ])

        list_facet_tags = []

        # It will always iterate only once
        for tags_facet in facet_query:
            list_facet_tags = tags_facet["tags_facet"]

        # Pre-processing phase

        # 1 - Remove lower/upper case redundancy
        lowerReduDict = dict()
        lowerReduSet = set()
        for i in range(len(list_facet_tags)):
            if list_facet_tags[i]["tags"] == list_facet_tags[i]["tags"].lower():
                lowerReduDict[list_facet_tags[i]["tags"]] = i

        for i in range(len(list_facet_tags)):
            tlw = list_facet_tags[i]["tags"].lower()

            if list_facet_tags[i]["tags"] != tlw and tlw in lowerReduDict.keys():
                list_facet_tags[lowerReduDict[tlw]]["count"] += list_facet_tags[i]["count"]
                lowerReduSet.add(i)
            elif list_facet_tags[i]["tags"] != tlw:
                list_facet_tags[i]["tags"] = tlw
                lowerReduDict[tlw] = i

        list_facet_tags = [list_facet_tags[i] for i in range(len(list_facet_tags)) if i not in lowerReduSet]

        # 2 - Add stressed words as "variant"
        if variant_analysis is True:
            stressRemDict = dict()
            stressRemSet = set()
            for i in range(len(list_facet_tags)):
                if list_facet_tags[i]["tags"] == Text.strip_accents(list_facet_tags[i]["tags"]):
                    stressRemDict[list_facet_tags[i]["tags"]] = i
                list_facet_tags[i]["variant_keys"] = []

            for i in range(len(list_facet_tags)):
                tsr = Text.__strip_accents(list_facet_tags[i]["tags"])

                if list_facet_tags[i]["tags"] != tsr and tsr in stressRemDict.keys():
                    list_facet_tags[stressRemDict[tsr]]["count"] += list_facet_tags[i]["count"]
                    list_facet_tags[stressRemDict[tsr]]["variant_keys"].append(list_facet_tags[i]["tags"])
                    stressRemSet.add(i)
                elif list_facet_tags[i]["tags"] != tsr:
                    list_facet_tags[i]["variant_keys"].append(list_facet_tags[i]["tags"])
                    list_facet_tags[i]["tags"] = tsr
                    stressRemDict[tsr] = i

            list_facet_tags = [list_facet_tags[i] for i in range(len(list_facet_tags)) if i not in stressRemSet]

        list_facet_tags.sort(reverse=True, key=lambda x: x['count'])

        return list_facet_tags


    def extract_collection(self, directory=None):
        """ extracts items from a given collection, and organizes it in files """

        # creates dir where things are going to be stored
        if directory is None:
            if not os.path.isdir(str(self.title)):
                directory = str(self.title)
            elif not os.path.isdir(str(self.title) + "_" + str(ownerId)):
                directory = str(self.title) + "_" + str(ownerId)
            else:
                directory = str(self.title) + "_" + str(ownerId) + str(Date.now())
                while(os.path.isdir(directory)):
                    time.sleep(1)
                    directory = str(self.title) + "_" + str(ownerId) + str(Date.now())

            directory = directory.replace(" ", "")

        OSUtils.createDir(directory)
        OSUtils.createDir(directory + "/" + IMAGEDIR)
        OSUtils.createDir(directory + "/" + VIDEODIR)
        OSUtils.createDir(directory + "/" + URLDIR)

        # header of the csv files
        head = HEAD_ITEMS
        head_media = HEAD_MEDIA
        head_list = HEAD_LINK_LIST
        head_set_links = HEAD_SET_LINKS
        head_set_urls = HEAD_SET_URLS

        # writes the headers
        CSVUtils.write_line_b_csv(directory + "/" + CSVITEMS, head, newfile=True)
        CSVUtils.write_line_b_csv(directory + "/" + CSVIMAGE, head_media, newfile=True)
        CSVUtils.write_line_b_csv(directory + "/" + CSVVIDEO, head_media, newfile=True)
        CSVUtils.write_line_b_csv(directory + "/" + CSVLINKS, head_list, newfile=True)
        CSVUtils.write_line_b_csv(directory + "/" + CSVSETLINKS, head_set_links,
                           newfile=True)
        CSVUtils.write_line_b_csv(directory + "/" + CSVSETURL, head_set_links,
                           newfile=True)

        # query - Item
        print("Fetching...")

        # for each item collected, verify if it matches the keywords and save in an
        # csv file the main attributes
        items_count = 0

        items = self.ContentGenerator()

        for it in items:

            line = []

            line.append(str(items_count))
            items_count += 1

            # if the fields exists in the json, write it on csv
            for field in ["title", "location", "publicationTime", "tags",
                          "mediaIds", "links", "source", "pageUrl"]:

                if field in it:
                    # convert data into human-readable format
                    if field == "publicationTime":
                        line.append(str(Date.tmiles2date(it["publicationTime"])))

                    # extra procedures if there is media related to the item
                    elif field == "mediaIds":
                        # write the number of media related
                        line.append(str(len(it["mediaIds"])))

                        m_ct = 1
                        for m_id in it["mediaIds"]:
                            mit = self.__mediaItem(m_id)

                            name_media = line[0] + "_" + str(m_ct)

                            if mit["type"] == "image":
                                CSVUtils.write_line_b_csv(directory + "/" + CSVIMAGE,
                                                   [name_media] + line[:-1] +
                                                   [mit["source"]] + [mit["url"]])

                            elif mit["type"] == "video":
                                CSVUtils.write_line_b_csv(directory + "/" + CSVVIDEO,
                                                   [name_media] + line[:-1] +
                                                   [mit["source"]] + [mit["url"]])

                            m_ct += 1

                    elif field == "links":
                        line.append(str(len(it["links"])))

                        if len(it["links"]) > 0:
                            basedir = "Links/" + line[0]

                            for lit in it["links"]:
                                CSVUtils.write_line_b_csv(directory + "/" + CSVLINKS,
                                                   [lit] +
                                                   [str(line[0])])

                    else:
                        line.append(str(it[field]))
                else:
                    # it indicates in the csv if there is no image or video,
                    # or if there is no link
                    if field == "mediaIds" or field == "links":
                        line.append(str(0))
                    else:
                        line.append("")

            CSVUtils.write_line_b_csv(directory + "/" + CSVITEMS, line)

        print("100.0% completed.")
