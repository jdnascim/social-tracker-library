""" Library created in order to communicate with Social Tracker """

import os
import datetime
import glob
import shutil
import csv
import requests
import math
import random
import filetype
import re
import cv2
import youtube_dl
import redis
import json
import time
from dateutil import parser
from pymongo import MongoClient
from skimage.measure import compare_ssim


# similarity limit to consider two images equal
__SSIM_THRESHOLD = 0.97

# conf file
__CONF_FILE = "conf.json"


def __demoConnection():
    """ returns database connection """
    with open(__CONF_FILE, 'r') as fp:
        conf = json.load(fp)

    client = MongoClient(
        conf["mongo"]["path"],
        username=conf["mongo"]["username"],
        password=conf["mongo"]["password"],
        authSource=conf["mongo"]["authsource"],
        unicode_decode_error_handler='ignore')

    # returning database
    return client.Demo


def __csvGenerator(csvfile):
    """ csv generator """

    f = open(csvfile, "r")
    f.readline()

    for line in csv.reader(f):
        yield line


def __different_images(link1, link2):
    """ Return True if two images are different """

    im1 = cv2.imread(link1)
    im2 = cv2.imread(link2)

    if im1.shape != im2.shape:
        return True

    im1 = cv2.cvtColor(im1, cv2.COLOR_BGR2GRAY)
    im2 = cv2.cvtColor(im2, cv2.COLOR_BGR2GRAY)

    # measure difference with structural similarity
    difference = compare_ssim(im1, im2)

    print(difference)

    # compare with threshold
    if difference > __SSIM_THRESHOLD:
        return False
    else:
        return True


# delete all rows given a item - used in case of keyboard
# interruption, as there may be corrupted files
def __csv_del_inconsistent_rows(it, csvfile="links.csv"):
    """ delete all rows in csv given a item - used in case of keyboardInt """

    inp = open(csvfile, 'r')
    output = open('.'+csvfile+"_tmp", 'w')
    writer = csv.writer(output)
    for row in csv.reader(inp):
        if row[1].isdigit() and int(row[1]) != it:
            writer.writerow(row)

    inp.close()
    output.close()
    shutil.move("."+csvfile+"_tmp", csvfile)


def __guessTypePath(path):
    """ "Guess" the type (used to discover if path is image or video) """

    try:
        mime = filetype.guess_mime(path)
        return mime[:mime.find("/")]
    except FileNotFoundError:
        return ""
    except Exception:
        raise


# This method is necessary, because youtube-dl adds extension
# to the filename after downloading it. Plus, it may works
# wrongly if a file with same name and different extension
# is created after the one required. It means that you are
# highly recommended use it just after downloading the media
def __fileplusextension(path):
    """ It returns path + .extension (e.g. im/im1 -> im/im1.jpeg) """

    most_recent_file = ""
    most_recent_time = 0

    for i in glob.glob(path+"*"):
        time = os.path.getmtime(i)
        if time > most_recent_time:
            most_recent_file = i
    return most_recent_file


def __removeDownloadedFile(path):
    """ remove file """
    try:
        os.remove(path)
    except Exception:
        pass


def __createDir(path):
    """ creates directory and doesn't raise Exception in FileExists """
    try:
        os.mkdir(path)
    except FileExistsError:
        pass
    except Exception:
        raise


def __link_directory(link, it):
    """ Given an item and link, returns the directory related to this pair """

    link_dir = str(link).replace("https:/", "").replace("http://", "")
    link_dir = link_dir.replace("www.", "").replace("/", "")
    link_dir = link_dir[:__lastocc(link, "/")]

    return "Links/" + str(it) + "/" + link_dir


def __write_line_b_csv(csvfile, line, newfile=False):
    """ write a entire line into the csv file """

    if newfile is True and os.path.isfile(csvfile) is True:
        os.remove(csvfile)

    with open(csvfile, 'a') as resultfile:
        wr = csv.writer(resultfile, dialect='excel')
        wr.writerow(line)


def __ogImageGenerator(link):
    """ given a link, try to get images from it by the parameter og:image """

    text = requests.get(link).text
    ini = text.find("\"og:image\"")
    end = 0
    while ini > -1:
        ini += 20 + end
        linklen = text[ini:].find("\"")
        end = ini + linklen

        yield text[ini:end]

        ini = text[end:].find("\"og:image\"")


def __youtube_link_download(link, it, csvfile, path=""):
    """ youtube download given a link
        used when scraping links (e.g scrap_link)"""

    try:
        name = __namefile(link)

        if path == "":
            path = __link_directory(link, it)

        arq = path + "/" + name

        if not os.path.isfile(arq):
            __youtube_download(link, arq)

            # -> ["name", "csvid", "source", "type", "path"]
            kind = __guessTypePath(arq)
            if kind == "video" or kind == "image":
                line = [name, str(it), link, kind, arq]
                __write_line_b_csv(csvfile, line)
            else:
                __removeDownloadedFile(arq)

    except Exception:
        __removeDownloadedFile(arq)
        pass


def __request_link_download(link, it, csvfile, path=""):
    """ request download given a link
        used when scraping links (e.g scrap_link)"""

    try:
        name = __namefile(link)

        if path == "":
            path = __link_directory(link, it)

        arq = path + "/" + name

        if not os.path.isfile(arq):
            open(arq, "wb").write(requests.get(link).content)

            # -> ["name", "csvid", "source", "type", "path"]
            kind = __guessTypePath(arq)
            if kind == "video" or kind == "image":
                line = [name, str(it), link, kind, arq]
                __write_line_b_csv(csvfile, line)
            else:
                __removeDownloadedFile(arq)
    except Exception as e:
        __removeDownloadedFile(arq)
        pass


def __lastocc(somestr, char):
    """ last occurence of a char in a string """

    return max(i for i, c in enumerate(somestr) if c == char)


def __namefile(link):
    """ get the name of a file given a link """

    return link[__lastocc(link, "/")+1:]


# developed in order to properly indexing media
def __media_next_count(path, it):
    """ return next 'id' available for media related to the item """

    return len(glob.glob(path+"/"+str(it)+"_*")) + 1


# return true if there is no similar media to file related
# to the item
def __link_file_unique(file, it, path):
    """ verify if file is unique in the path """

    for i in glob.glob(path+"/"+it+"_*"):
        if __different_images(file, i) is False:
            return False
    return True


def __date2tmiles(dat):
    """ return timemiles from a date at the same format used in the system """
    return int(parser.parse(str(dat)).strftime("%s")) * 1000


def __tmiles2date(tmiles):
    """ return date from timemiles """
    return datetime.datetime.fromtimestamp(int(tmiles)/1.e3)


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


def scrap_link(link, it, csvfile="links.csv"):
    """ given a link, try to get items from this link """

    try:
        __createDir("Links/"+it)
        directory = __link_directory(link, it)
        __createDir(directory)

        __youtube_link_download(link, it, csvfile)

        for im in __ogImageGenerator(link):
            __request_link_download(im, it, csvfile, directory)

        print('\x1b[6;30;42m' + "Scrap Finished for Link " + str(it) +
              '\x1b[0m')

    except KeyboardInterrupt:
        # if occurs a keyboard interruption, delete all media related to the
        # given item
        shutil.rmtree("Links/"+it, ignore_errors=True)
        __csv_del_inconsistent_rows(csvfile)
        raise
    except Exception as e:
        print(e)


# compares if media on imagepath and videopath is similar to media in linkspath
# have already been collected. If so, removes the collected media by links
def __remove_duplicate_media(imagepath="Images", videopath="Videos",
                             linkspath="Links", csvlinks="links.csv"):
    """ used to avoid duplicates between Image/Video items and Link items """

    print('\x1b[6;30;42m' + "Removing Duplicated Media." + '\x1b[0m')
    links = open(csvlinks, "r")
    newcsvlinks = "." + csvlinks + "_tmp"

    for line in csv.reader(links):
        if line[3] == "image":
            path = imagepath
        elif line[3] == "video":
            path = videopath

        if os.path.isfile(line[4]):
            if __link_file_unique(line[4], line[1], path):
                __write_line_b_csv(newcsvlinks, line)
            else:
                print(line[1])
                os.remove(line[4])

    shutil.move(newcsvlinks, csvlinks)
    print('\x1b[6;30;42m' + "Done." + '\x1b[0m')


# write the links header into the csv
def init_linkscsv(csvfile="links.csv", create_if_exists=False):
    if create_if_exists or (os.path.isfile(csvfile) is False):
        head_media = ["name", "csvid", "source", "type", "path"]
        os.system("rm " + csvfile)
        __write_line_b_csv(csvfile, head_media)


def merge_media(csvimage="image.csv", csvvideo="video.csv",
                csvlinks="links.csv", csvitems="items.csv", imagepath="Images",
                videopath="Videos", linkspath="Links"):
    """ given items in Image, Video and Link directory, it executes a merge
    operation, keeping all media in Image and Video directories """

    __remove_duplicate_media()

    print('\x1b[6;30;42m' + "Merging..." + '\x1b[0m')

    image = csv.reader(open(csvimage, "r"))
    video = csv.reader(open(csvvideo, "r"))
    links = csv.reader(open(csvlinks, "r"))
    item = csv.reader(open(csvitems, "r"))

    outputimage = "."+csvimage+"_tmp"
    outputvideo = "."+csvvideo+"_tmp"

    __write_line_b_csv(outputimage, next(image, None))
    __write_line_b_csv(outputvideo, next(video, None))

    next(links, None)
    next(item, None)
    imageline = next(image, None)
    videoline = next(video, None)
    itemline = next(item, None)

    for linkline in links:

        if linkline[3] == "image":

            while ((not isinstance(imageline, type(None))) and
                    int(imageline[1]) <= int(linkline[1])):

                __write_line_b_csv(outputimage, imageline)
                imageline = next(image, None)

            while ((not isinstance(itemline, type(None))) and
                    int(itemline[0]) < int(linkline[1])):

                itemline = next(item, None)

            name_file = (imagepath + "/" + itemline[0] + "_" + str(
                __media_next_count(imagepath, linkline[1])))

            line = ([name_file[__lastocc(name_file, "/")+1:]] + itemline[:5] +
                    [linkline[2]])

            shutil.move(linkline[4], name_file)

            __write_line_b_csv(outputimage, line)

        elif linkline[3] == "video":

            while ((not isinstance(videoline, type(None))) and
                    int(videoline[1]) <= int(linkline[1])):

                __write_line_b_csv(outputvideo, videoline)
                videoline = next(video, None)

            while ((not isinstance(itemline, type(None))) and
                   int(itemline[0]) < int(linkline[1])):

                itemline = next(item, None)

            name_file = (videopath+"/"+itemline[0] + "_" + str(
                __media_next_count(videopath, linkline[1])))

            line = ([name_file[__lastocc(name_file, "/")+1:]] + itemline[:5] +
                    [linkline[2]])

            shutil.move(linkline[4], name_file)

            __write_line_b_csv(outputvideo, line)

    shutil.move(outputimage, csvimage)
    shutil.move(outputvideo, csvvideo)
    print('\x1b[6;30;42m' + "Done." + '\x1b[0m')


def __youtube_download(link, output=None, noplaylist=True):
    """ youtube download """

    try:
        if not os.path.isfile(output):
            youtube_dl.YoutubeDL({'outtmpl': output,
                                  'noplaylist': True}).download([link])

            shutil.move(__fileplusextension(output), output)
        else:
            print("File " + output + " exists.")
    except KeyboardInterrupt:
        if os.path.isfile(__fileplusextension(output)):
            os.remove(__fileplusextension(output))
        raise
    except Exception:
        raise


def __request_download(link, output):
    """request download"""

    print(os.getcwd())
    print(link, output)
    try:
        if not os.path.isfile(output):
            open(output, "wb").write(requests.get(link).content)
            print(output)
        else:
            print("File " + output + " exists.")
    except KeyboardInterrupt:
        if os.path.isfile(__fileplusextension(output)):
            os.remove(__fileplusextension(output))
        raise
    except Exception:
        raise


def media_csv_download(csvfile, type, directory="."):
    """ download media from the csv generated
        type parameter: (I)mage or (V)ideo """

    valid_types = {"i", "image", "v", "video"}

    type = type.lower()

    if type not in valid_types:

        raise ValueError("Parameter type must be one of %r." % valid_types)
    if directory[-1] == "/":
        directory = directory[:-1]

    try:
        if type == "i" or type == "image":
            for line in __csvGenerator(directory + "/" + csvfile):
                    __request_download(line[7], directory + "/Images/" +
                                       line[0])

        elif type == "v" or type == "video":
            for line in __csvGenerator(directory + "/" + csvfile):
                    __youtube_download(line[7], directory + "/Videos/" +
                                       line[0])

    except KeyboardInterrupt:
        print("\nStopping...")
    except Exception:
        raise


def list_collections():
    """ lists current collections in the system """

    db_m = __demoConnection()

    collection_settings = db_m.Collection.find()

    for col in collection_settings:
        print("title:", str(col["title"]))
        print("owner:", str(col["ownerId"]))
        print("keywords:", str(col["keywords"]))
        print("accounts:", str(col["accounts"]))
        print("status:", str(col["status"]))
        print("locations:", str(col["nearLocations"]))
        print("")


def __keywordsGenerator(keywords):
    """ generator for collection["keywords"] """

    for key in keywords:
        yield str(key["keyword"])


def __accountIDGenerator(accounts):
    """ generator for collection["accounts"] """

    for acc in accounts:
        yield str(acc["source"]) + "#" + str(acc["id"])


# rules: if it matchs one of the keywords, it is in
#        if one keyword has two or more words, all words have to be in the text
#           in order to exist a match
def __cleanTitleMatch(cleanTitle, keywords):
    """ verifies if cleanTitle of item matches one of the keywords """

    for key in __keywordsGenerator(keywords):
        match = True
        words = key.split()
        for wd in words:
            if wd not in cleanTitle:
                match = False
                break
        if match is True:
            return True
    return False


# rules: if it matchs one of the users, it is in
def __accountMatch(uid, accounts):
    """ verifies if user of the item matches the user set in the collection """

    for acc in __accountIDGenerator(accounts):
        if acc == uid:
            return True
    return False


def __item_query(title, ownerId, start_date=None, end_date=None,
                 original=True):
    """ given the name of a colleciton, its owner and start/end date, returns
        the query to pass as parameter of db.Collection.Find() """

    db_m = __demoConnection()

    # get the register concerning the collection passed as parameter
    collection_settings = db_m.Collection.find({
                                            "ownerId": ownerId,
                                            "title": title
                                             })[0]

    # generate the query
    items_query = dict()

    items_query["title"] = __collection_regstr_query(
        collection_settings["keywords"])

    items_query["original"] = original

    if start_date is not None:
        items_query["publicationTime"] = {"$gt": __date2tmiles(start_date)}
    else:
        items_query["publicationTime"] = {"$gt": collection_settings["since"]}

    if end_date is not None:
        items_query["publicationTime"] = {"$lte": __date2tmiles(end_date)}

    return items_query


def collection_item_count(title, ownerId, start_date=None, end_date=None,
                          original=True):
        """ count the qtde of items given a collection """

        # connection with Mongo
        db_m = __demoConnection()

        item_query = __item_query(title, ownerId, start_date, end_date)

        return db_m.Item.find(item_query).count()


def __collectionContentGenerator(title, ownerId, start_date=None,
                                 end_date=None):
    """ generator of the content of a given collection """

    # connection with Mongo
    db_m = __demoConnection()

    item_query = __item_query(title, ownerId, start_date, end_date)

    items = db_m.Item.find(item_query)

    for it in items:
        yield it


def __items_tags_facet_query(title, ownerId, start_date=None, end_date=None,
                             original=True):
    """ facet query in 'tags' field of Item collection. """
    db_m = __demoConnection()

    item_query = __item_query(title, ownerId, start_date, end_date, original)

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

    return list_facet_tags


def __mediaItem(m_id):
    """ returns mediaItem given id """

    db_m = __demoConnection()

    return db_m.MediaItem.find({"_id": m_id})[0]


def __expandURL(link):
    """ returns "true" link to a page """
    try:
        return requests.get(link).url
    except Exception:
        return link


def expand_url_links(link_list="link_list.csv"):
    """ given the initial link_list extracted, expand its urls. it is
    recommended to execute this routine, in order to avoid downloading the
    same media more than once. Furthermore, it avoids dependance of a short
    link """

    try:
        __write_line_b_csv(link_list + "_tmp", ["link", "store"], newfile=True)

        for line in __csvGenerator(link_list):
            url_line = __expandURL(line[0])
            print(line[1])
            __write_line_b_csv(link_list + "_tmp", [url_line, line[1]])

        shutil.move(link_list + "_tmp", link_list)
    except Exception:
        os.remove(link_list+"_tmp")


def extract_collection(title, ownerId, start_date=None, end_date=None):
    """ extracts items from a given collection, plus generates scripts to
    download media """

    # creates dir where things are going to be stored
    directory = str(title) + "_" + str(ownerId)

    directory = directory.replace(" ", "")

    __createDir(directory)
    __createDir(directory + "/Images")
    __createDir(directory + "/Videos")
    __createDir(directory + "/Links")

    # header of the csv files
    head = ["csvid", "text", "location", "pubtime", "tags", "media"]
    head_media = ["name", "csvid", "text", "location", "pubtime", "tags",
                  "source", "url"]

    head_list = ["link", "store"]

    # writes the headers
    __write_line_b_csv(directory + "/items.csv", head, newfile=True)
    __write_line_b_csv(directory + "/image.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/video.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/link_list.csv", head_list, newfile=True)

    # query - Item
    print("Fetching...")

    # for each item collected, verify if it matches the keywords and save in an
    # csv file the main attributes
    items_count = 0

    items = __collectionContentGenerator(title, ownerId, start_date, end_date)

    for it in items:

        line = []

        line.append(str(items_count))
        items_count += 1

        # if the fields exists in the json, write it on csv
        for field in ["title", "location", "publicationTime", "tags",
                      "mediaIds", "links"]:

            if field in it:
                # convert data into human-readable format
                if field == "publicationTime":
                    line.append(str(__tmiles2date(it["publicationTime"])))

                # extra procedures if there is media related to the item
                elif field == "mediaIds":
                    # write the number of media related
                    line.append(str(len(it["mediaIds"])))

                    m_ct = 1
                    for m_id in it["mediaIds"]:
                        mit = __mediaItem(m_id)

                        name_media = line[0] + "_" + str(m_ct)

                        if mit["type"] == "image":
                            __write_line_b_csv(directory + "/image.csv",
                                               [name_media] + line[:-1] +
                                               [mit["source"]] + [mit["url"]])

                        elif mit["type"] == "video":
                            __write_line_b_csv(directory + "/video.csv",
                                               [name_media] + line[:-1] +
                                               [mit["source"]] + [mit["url"]])

                        m_ct += 1

                elif field == "links":
                    line.append(str(len(it["links"])))

                    if len(it["links"]) > 0:
                        basedir = "Links/" + line[0]

                        for lit in it["links"]:
                            __write_line_b_csv(directory + "/link_list.csv",
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

        __write_line_b_csv(directory + "/items.csv", line)

    print("100.0\% \completed.")


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


def query_expansion_tags(title, ownerId, tag_min_frequency=0.05,
                         start_date=None, end_date=None, original=True):
    """ applies query expansion related to tags """

    if end_date is None:
        end_date = datetime.datetime.now()

    time.sleep(2)

    ct = collection_item_count(title, ownerId, start_date, end_date)

    if ct > 0:

        list_facet_tags = __items_tags_facet_query(title, ownerId, start_date,
                                                   end_date, original)

        new_keywords = []

        i = 0

        while (i < len(list_facet_tags)
               and list_facet_tags[i]["count"]/ct >= tag_min_frequency):

            new_keywords.append(list_facet_tags[i]["tags"])
            i += 1

        collection_add_keywords(title, ownerId, new_keywords)
    else:
        print("Empty Collection")
