""" Library created in order to communicate with Social Tracker """

import os
import datetime
import glob
import shutil
import csv
import requests
import math
import random
import unicodedata
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
from lxml import html


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

    if char in somestr:
        return max(i for i, c in enumerate(somestr) if c == char)
    else:
        return -1


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


            if bool(filetype.guess_mime(output)) is True:
                print(link, output)
                return True
            else:
                return False
        else:
            print("File " + output + " exists.")
            return False
    except KeyboardInterrupt:
        if os.path.isfile(__fileplusextension(output)):
            os.remove(__fileplusextension(output))
        raise
    except Exception:
        pass


def __request_download(link, output):
    """request download"""

    try:
        if not os.path.isfile(output):
            open(output, "wb").write(requests.get(link).content)

            if bool(filetype.guess_mime(output)) is True:
                print(link, output)
                return True
            else:
                return False
        else:
            print("File " + output + " exists.")
            return False
    except KeyboardInterrupt:
        if os.path.isfile(__fileplusextension(output)):
            os.remove(__fileplusextension(output))
        raise
    except Exception:
        raise


def media_csv_download(csvfile, type_file="", directory=".", csvset="set_links.csv"):
    """ download media from the csv generated
        type parameter: (I)mage or (V)ideo """

    valid_types = {"i", "image", "v", "video"}

    # if type not specified, get the type by the name of the csvfile
    if type_file == "":
        type_file = csvfile[:__lastocc(csvfile, ".")]

    type_file = type_file.lower()

    if type_file not in valid_types:
        raise ValueError("Parameter type_file/csvfile must be one of %r." %
                         valid_types)

    if directory[-1] == "/":
        directory = directory[:-1]

    # now it iterates through the csvfile [__csvGenerator()] and download the
    # items [*.download() -> depends on the type specified]
    try:
        if type_file == "i" or type_file == "image":
            for line in __csvGenerator(directory + "/" + csvfile):
                linkfile = directory + "/Images/" + line[0]

                chk = __request_download(line[7], linkfile)

                if chk is True:
                    __write_line_b_csv(csvset, [__expandURL(line[7]), line[1], linkfile])

        elif type_file == "v" or type_file == "video":
            for line in __csvGenerator(directory + "/" + csvfile):
                linkfile = directory + "/Videos/" + line[0]

                chk = __youtube_download(line[7], linkfile)

                if chk is True:
                    __write_line_b_csv(csvset, [__expandURL(line[7]), line[1], linkfile])
    # PRÓXIMO PASSOS:
    # - CASO ESTEJA OK, IMPLEMENTAR AS ALTERAÇÕES NECESSÁRIAS PARA O DOWNLOAD
    #   REFERENTE A LINKS
    # - VERIFICAR A QUESTÃO DA BIBLIOTECA ARTICLE - TALVEZ IMPLEMENTAR
    except KeyboardInterrupt:
        print("\nStopping...")
    except Exception as e:
        print(e)
        raise


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
        print("")


def list_extracted_collections():
    """ lists every extracted collection in the directory
        configured in __CONF_FILE """

    print("List of the extracted collections")
    print()

    with open(__CONF_FILE, 'r') as fp:
        conf = json.load(fp)

    path = conf["collections"]["path"]

    try:
        dir_list = next(os.walk(path))[1]
        for dire in dir_list:
            if dire[0] != '.':
                print(dire)
    except Exception:
        pass


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
                 original=True, logfile=None):
    """ given the name of a colleciton, its owner and start/end date, returns
        the query to pass as parameter of db.Collection.Find() """

    db_m = __demoConnection()

    # get the register concerning the collection passed as parameter
    collection_settings = db_m.Collection.find({
                                            "ownerId": ownerId,
                                            "title": title
                                             })[0]

    # write log if specified
    if logfile != "" and logfile is not None:
        # remove unnecessary fields to the logfile
        del collection_settings['_id']
        del collection_settings['status']
        del collection_settings['ownerId']

        # turn dates into readable format
        collection_settings['creationDate'] = str(__tmiles2date(collection_settings['creationDate']))
        collection_settings['updateDate'] = str(__tmiles2date(collection_settings['updateDate']))

        if start_date != None:
            collection_settings['since'] = start_date
        else:
            collection_settings['since'] = str(__tmiles2date(collection_settings['since']))

        if end_date != None:
            collection_settings['until'] = end_date

        collection_settings['original'] = original

        with open(logfile, "w") as logf:
            logf.write(json.dumps(collection_settings, indent=2))


    # generate the query
    items_query = dict()

    items_query["title"] = __collection_regstr_query(
        collection_settings["keywords"])

    # if original is true, query should return only orginal. If orginal is
    # false, query should ignore whether item is original or not
    if original is True:
        items_query["original"] = original

    if start_date is not None:
        items_query["publicationTime"] = {"$gte": __date2tmiles(start_date)}
    else:
        items_query["publicationTime"] = {"$gte": collection_settings["since"]}

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
                                 end_date=None, logfile=""):
    """ generator of the content of a given collection """

    # connection with Mongo
    db_m = __demoConnection()

    item_query = __item_query(title, ownerId, start_date, end_date, logfile=logfile)

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
    stressRemDict = dict()
    stressRemSet = set()
    for i in range(len(list_facet_tags)):
        if list_facet_tags[i]["tags"] == __strip_accents(list_facet_tags[i]["tags"]):
            stressRemDict[list_facet_tags[i]["tags"]] = i
        list_facet_tags[i]["variant_keys"] = []

    for i in range(len(list_facet_tags)):
        tsr = __strip_accents(list_facet_tags[i]["tags"])

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


def __strip_accents(s):
    """ given a string, returns it without any stress """

    return ''.join(c for c in unicodedata.normalize('NFD', s)
        if unicodedata.category(c) != 'Mn').lower()


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


def __expand_url_links(link_list="link_list.csv"):
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
    """ extracts items from a given collection, and organizes it in files """

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

    head_set_links = ["link", "item_id", "path"]

    # writes the headers
    __write_line_b_csv(directory + "/items.csv", head, newfile=True)
    __write_line_b_csv(directory + "/image.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/video.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/link_list.csv", head_list, newfile=True)
    __write_line_b_csv(directory + "/set_links.csv", head_set_links,
                       newfile=True)

    # query - Item
    print("Fetching...")

    # for each item collected, verify if it matches the keywords and save in an
    # csv file the main attributes
    items_count = 0

    items = __collectionContentGenerator(title, ownerId, start_date, end_date, logfile=directory+ "/logfile.json")

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

    print("100.0\% completed.")


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


def __csv_to_dict(csvfile, id_key, id_value):
    """ given a csv file, return a dict based upon it """

    csvgen = __csvGenerator(csvfile)

    next(csvgen)
    csvdict = dict()

    for row in csvgen:
        csvdict[row[id_key]] = row[id_value]

    return csvdict


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


def __reduced_text(text):
    """ returns True if the twitter item (text) likely has more that 140
    character (details about it in the expand_text description)"""

    if len(text) >= 140 and "..." in text:
        return True
    else:
        return False


def expand_texts(csvitems="items.csv", csvlinks="link_list.csv"):
    """ when a twitter item has more that 140 characters, only 140 characters
    are stored in the database, and a link to the tweet is stored in the
    "links" field. Obviously, if only a partial text is in the database, only
    this partial text is going to be in the csv file. This functions aims to
    scrap the full text of these items using the link to the tweet."""

    LINK_PATTERN = "https://twitter.com/i/web/status/"

    last_dot = __lastocc(csvitems, ".")

    if last_dot == -1:
        last_dot = len(csvitems)

    AUG_IT = csvitems[:last_dot] + "_AUG_TEXT" + csvitems[last_dot:]

    # STEP 1: Verify which items may need an text expansion
    texts_dict = dict()

    for it in __csvGenerator(csvitems):
        if __reduced_text(it[1]):
            texts_dict[int(it[0])] = ""

    # STEP 2: Get the text of these items
    for lnk in __csvGenerator(csvlinks):
        if int(lnk[1]) in texts_dict.keys() and lnk[0][:33] == LINK_PATTERN:

            texts_dict[int(lnk[1])] = int(lnk[0][33:])

    # STEP 3: Create a new item file
    itemsgen = __csvGenerator(csvitems)

    # Write the header in the new file
    __write_line_b_csv(AUG_IT, next(itemsgen), True)

    for it in itemsgen:
        if int(it[0]) in texts_dict.keys():
            full_text = __full_tweet_text(LINK_PATTERN + str(texts_dict[int(it[0])]))

            if len(full_text) > len(it[1]):
                __write_line_b_csv(AUG_IT, it[:1] + [full_text] + it[2:])
                print(it[0], full_text)
            else:
                __write_line_b_csv(AUG_IT, it)
                print(it[0], it[1])
        else:
            __write_line_b_csv(AUG_IT, it)


def __full_tweet_text(link_t):
    """ given a link to a tweet, extract its entire text. It is necessary for
    twitter items which has more than 140 characters """

    page = requests.get(link_t)
    tree = html.fromstring(page.content)
    text = tree.xpath('//div[contains(@class, "permalink-tweet-container")]//p[contains(@class, "tweet-text")]//text()')

    for i in range(len(text)):
        if text[i][:4] == "pic." or text[i][:7] == "http://" or text[i][:4] == "www." or text[i][:8] == "https://":

            text[i] = " " + text[i]

    return "".join(text)


def query_expansion_tags(title, ownerId, start_date=None, end_date=None,
                         original=True, tag_min_frequency=0.005, ask_conf=True):
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

            if ask_conf is False:
                new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    new_keywords.append(k)
            else:
                if str(input("add " + str(list_facet_tags[i]["tags"]) + "? (y/n)\n")) == "y":
                    new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    if str(input("add variant " + str(k) + "? (y/n)\n")) == "y":
                        new_keywords.append(k)

            i += 1

        collection_add_keywords(title, ownerId, new_keywords)
    else:
        print("Empty Collection")
