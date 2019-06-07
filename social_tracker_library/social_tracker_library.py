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
import string
import operator
import itertools
import twokenize
from newspaper import Article
from bs4 import BeautifulSoup
from dateutil import parser
from pymongo import MongoClient
from skimage.measure import compare_ssim
from lxml import html


# similarity limit to consider two images equal
__SSIM_THRESHOLD = 0.97

# conf file
__CONF_FILE = os.path.dirname(os.path.realpath(__file__)) + "/" + "conf.json"


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


def __csvGenerator(csvfile, delimiter=",", hide_header=True):
    """ csv generator """

    f = open(csvfile, "r")

    if hide_header is True:
        f.readline()

    for line in csv.reader(f, delimiter=delimiter):
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


def __urlImageGenerator(link):
    """ given a link, try to get images from it by the parameter og:image """
    a = Article(url=link)
    a.download()
    a.parse()
    a.fetch_images()

    for img in a.imgs:
        yield img


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

    #TODO: require YYYY-MM-DD format

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


def links_media(csvfile="links.csv"):
    """ scrap the links """

    try:
        __createDir("Links/"+it)
        directory = __link_directory(link, it)
        __createDir(directory)

        __youtube_link_download(link, it, csvfile)

        for im in __urlImageGenerator(link):
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


def __scrap_link(link, it, csvfile="links.csv"):
    """ given a link, try to get items from this link """

    try:
        __createDir("Links/"+it)
        directory = __link_directory(link, it)
        __createDir(directory)

        __youtube_link_download(link, it, csvfile)

        for im in __urlImageGenerator(link):
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


def __youtube_download(link, output=None, noplaylist=True, overwrite=False):
    """ youtube download """

    try:
        if os.path.isfile(output) is False or overwrite is True:
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


def __request_download(link, output, overwrite=False):
    """request download"""

    try:
        if os.path.isfile(output) is False or overwrite is True:
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


def __decision(probability):
    """ returns True or False. Probability defines the chances to this
        function returns True"""

    return random.random() < probability


def media_csv_download(csvfile, type_file="", directory="", csvset="",
                       from_beginning=False, sample=False, overwrite=False):
    """ download media from the csv generated
        type parameter: (I)mage or (V)ideo
        from_beginning: defines if it will start based on the medialog_file
        sample: if it is a number, download (randomly and approximately) only
                total*sample items """

    # verify if sample is valid
    sample_mode = False
    if type(sample) == float:
        from_beginning = True
        sample_mode = True
    elif sample is not False and sample is not None:
        print("Invalid Sample - Ignoring...")

    # if overwrite True, activate from_beginning
    if overwrite is True:
        from_beginning = True

    valid_types = {"i", "image", "v", "video"}

    # if type not specified, get the type by the name of the csvfile
    if type_file == "":
        type_file = csvfile[__lastocc(csvfile,"/")+1:__lastocc(csvfile, ".")]

    type_file = type_file.lower()

    if type_file not in valid_types:
        raise ValueError("Parameter type_file/csvfile must be one of %r." %
                         valid_types)

    # if the directory was not passed as parameter, assumes the directory as
    # the same of the csvfile
    if directory == "":
        directory = csvfile[:__lastocc(csvfile,"/")]
        csvfile = csvfile[__lastocc(csvfile,"/")+1:]
    elif directory[-1] == "/":
        directory = directory[:-1]

    if csvset == "":
        csvset = directory + "/set_links.csv"

    # now it iterates through the csvfile [__csvGenerator()] and download the
    # items [*.download() -> depends on the type specified]

    csvGen = __csvGenerator(directory + "/" + csvfile)

    # if from_beginning = False, try to start from the file where it
    # stopped in the former iteration
    medialog_file = directory + "/medialog.json"

    if os.path.isfile(medialog_file) is False:
        with open(medialog_file, 'w') as medialog:
            json.dump(dict(), medialog)

    with open(medialog_file, 'r') as logfile:
        medialog = json.load(logfile)

    #TODO Add % completed
    try:
        if type_file == "i" or type_file == "image":
            if from_beginning == False:
                if "last_image" in medialog.keys():
                    last_image = str(medialog["last_image"])
                    last_image_fl = float(last_image.replace("_","."))
                else:
                    last_image = "0_0"
                    last_image_fl = 0.0

                if last_image_fl > 0.0:
                    for line in csvGen:
                        if float(str(line[0]).replace("_",".")) == last_image_fl:
                            print("Skipping until", line[0])
                            break
                        elif float(str(line[0]).replace("_",".")) > last_image_fl:
                            print(float(str(line[0]).replace("_",".")), last_image_fl)
                            print("Error: Last image in log does not exist - Starting from the beginning")
                            csvGen = __csvGenerator(directory + "/" + csvfile)
                            break

            for line in csvGen:
                linkfile = directory + "/Images/" + line[0]

                try:
                    # if sample mode is set, download only a small number of
                    # items, based on the sample probability
                    if sample_mode is True:
                        if __decision(probability=sample) is False:
                            continue

                    chk = __request_download(line[7], linkfile, overwrite=overwrite)

                    if chk is True:
                        __write_line_b_csv(csvset, [__expandURL(line[7]), line[0], linkfile])
                        last_image = line[0]
                except requests.exceptions.ConnectionError:
                    continue

            # set the last image downloaded after the end of the loop
            if sample_mode is False:
                __add_keyval_json("last_image", last_image, medialog_file)

        elif type_file == "v" or type_file == "video":
            if from_beginning == False:
                if "last_video" in medialog.keys():
                    last_video = str(medialog["last_video"])
                    last_video_fl = float(last_video.replace("_","."))
                else:
                    last_video = "0_0"
                    last_video_fl = 0.0

                if last_video_fl > 0.0:
                    for line in csvGen:
                        if float(str(line[0]).replace("_",".")) == last_video_fl:
                            print("Skipping", line[0])
                            break
                        elif float(str(line[0]).replace("_",".")) > last_video_fl:
                            print("Error: Last video in log does not exist - Starting from the beginning")
                            csvGen = __csvGenerator(directory + "/" + csvfile)
                            break

            for line in csvGen:
                # if sample mode is set, download only a small number of
                # items, based on the sample probability
                if sample_mode is True:
                    if __decision(probability=sample) is False:
                        continue

                linkfile = directory + "/Videos/" + line[0]

                chk = __youtube_download(line[7], linkfile, overwrite=overwrite)

                if chk is True:
                    __write_line_b_csv(csvset, [__expandURL(line[7]), line[1], linkfile])

                    last_video = line[0]

            # set the last video downloaded after the end of the loop
            if sample_mode is False:
                __add_keyval_json("last_video", last_video, medialog_file)
    except KeyboardInterrupt:
        print("\nStopping...")

        # set last item succesfully downloaded in the medialog file:
        if sample_mode is False:
            if type_file == "i" or type_file == "image":
                __add_keyval_json("last_image", last_image, medialog_file)
            elif type_file == "v" or type_file == "video":
                __add_keyval_json("last_video", last_video, medialog_file)
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


def list_extracted_collections(path=None):
    """ lists every extracted collection in the directory
        configured in __CONF_FILE """

    print("List of the extracted collections")
    print()

    if path is None:
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


def __add_keyval_json(key, value, jsonfile):
    """ add a key-value to a json file """

    if os.path.isfile(jsonfile):
        with open(jsonfile) as f:
            data = json.load(f)
    else:
        data = dict()

    data[key] = value

    with open(jsonfile, 'w') as f:
        json.dump(data, f, indent=2)


def __read_keyval_json(key, jsonfile):
    """ read a key-value of a json file  """

    with open(jsonfile) as f:
        data = json.load(f)

    if key in data.keys():
        return data[key]
    else:
        return ""


def collection_item_count(title, ownerId, start_date=None, end_date=None,
                          original=True):
        """ count the qtde of items given a collection """

        # connection with Mongo
        db_m = __demoConnection()

        item_query = __item_query(title, ownerId, start_date, end_date)

        return db_m.Item.find(item_query).count()


def __collectionContentGenerator(title, ownerId, start_date=None,
                                 end_date=None, original=True):
    """ generator of the content of a given collection """

    # connection with Mongo
    db_m = __demoConnection()

    item_query = __item_query(title, ownerId, start_date, end_date, original)

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
        __write_line_b_csv(link_list + "_tmp", ["link", "csvid_dir"], newfile=True)

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
    head = ["csvid", "text", "location", "pubtime", "tags", "media", "source", "itemUrl"]
    head_media = ["name", "csvid", "text", "location", "pubtime", "tags",
                  "source", "url"]

    head_list = ["link", "csvid_dir"]

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

    items = __collectionContentGenerator(title, ownerId, start_date, end_date)

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

    print("100.0% completed.")


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


def __csv_to_dict(csvfile, id_key, id_value, delimiter=','):
    """ given a csv file, return a dict based upon it """

    csvgen = __csvGenerator(csvfile, delimiter=delimiter)

    csvdict = dict()

    for row in csvgen:
        csvdict[row[id_key]] = row[id_value]

    #TODO if every key is digit, turn into int
    #TODO id_value should be a list of index
    #TODO increment delimiter parameter (delimiter=',')

    return csvdict


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


def __reduced_text(text):
    """ returns True if the twitter item probably is shortened.
        Once the criterium was a little bit more complex, this
        is the reason why a nowadays quite simple function
        like this one was developed """

    #if "…http" in text.replace(" ", ""):

    if "…" in text:
        return True
    else:
        return False


def expand_texts(csvitems="items.csv", medialog_file="", from_beginning=False):
    """ Sometimes, a Twitter item's text is not entirely stored in the
        database. This function aims to scrap the tweet url in order to
        get the full text """


    # PRE: determine the directory and the name of the file of augmented texts
    last_dot = __lastocc(csvitems, ".")

    if last_dot == -1:
        last_dot = len(csvitems)

    AUG_IT = csvitems[:last_dot] + "_AUG_TEXT" + csvitems[last_dot:]

    # find the log_file
    if medialog_file == "":
        directory = csvitems[:__lastocc(csvitems,"/")]
        medialog_file = directory + "/medialog.json"

    # write the header in the new file
    itemsgen = __csvGenerator(csvitems)

    # reach the items generator to the start point (according to whether it
    # should start from the beginning or not)
    if from_beginning is False and os.path.isfile(AUG_IT) is True:
        last_text = __read_keyval_json("last_text", medialog_file)

        if last_text != "":

            last_text = int(last_text)
            error = False
            for it in itemsgen:
                if int(it[0]) == last_text:
                    print("Starting from item:", last_text + 1)
                    break
                elif int(it[0]) > last_text:
                    print("LOGFILE ERROR - Starting from the beginning")
                    error = True
                    break

            if error is True:
                itemsgen = __csvGenerator(csvitems)
                next(itemsgen)
        else:
            last_text = 0
    else:
        # write header of csvitem into csvitem_AUG
        __write_line_b_csv(AUG_IT, next(csv.reader(open(csvitems, "r"))), True)
        last_text = 0

    # for each item, verify if its text should be expanded
    try:
        #TODO - Add %
        for it in itemsgen:
            if __reduced_text(it[1]):
                #full_text = __full_tweet_text(it[1][__lastocc(it[1],"…")+1:].strip()
                #print(it[8])
                full_text = __full_tweet_text(it[8])

                if full_text != "":
                    # write the expanded text
                    __write_line_b_csv(AUG_IT, it[:1] + [full_text] + it[2:])
                    print(it[0], full_text)
                else:
                    __write_line_b_csv(AUG_IT, it)
                    print(it[0], it[1])
            else:
                __write_line_b_csv(AUG_IT, it)

            last_text = it[0]

        # set the last text after finish the loop.
        __add_keyval_json("last_text", last_text, medialog_file)
        print("Finished")
    except KeyboardInterrupt:
        print("\nStopping...")

        # set last verified text in the medialog file:
        __add_keyval_json("last_text", last_text, medialog_file)
    except Exception as e:
        print(e)
        __add_keyval_json("last_text", last_text, medialog_file)
        raise


def __cleanText(text):
    """ given raw text, return clean text, following the same procedure
    which is done by the stream manager """

    # 1 - clean
    text = unicodedata.normalize("NFD", text)
    text = re.sub(r"\\p{Cntrl}", "", text)
    text = re.sub(r"\\n", " ", text)
    text = re.sub(r"\\.{2,}", ". ", text)

    # 2 - toLowerCase
    text = text.lower()

    # 2.5 - remove retweet indication
    if text.startswith("rt @"):
        ssp = text[3:].find(" ")
        text = text[ssp+4:]

    # 3 - normalize
    text = re.sub(r"i'm", "i am", text);
    text = re.sub(r"it's", "it is", text);
    text = re.sub(r"what's", "what is", text);
    text = re.sub(r"don't", "do not", text);
    text = re.sub(r"dont ", "do not ", text);

    # 4 - join tokens
    tokens = twokenize.tokenize(text)
    tokens = [t for t in tokens if not (t.startswith("https://") or t in string.punctuation)]

    return " ".join(tokens)


def __full_tweet_text(link_t):
    """ given a link to a tweet, extract its entire text. It is necessary for
    twitter items which has more than 140 characters """

    try:
        page = requests.get(link_t)
        tree = html.fromstring(page.content)
        text = tree.xpath('//div[contains(@class, "permalink-tweet-container")]//p[contains(@class, "tweet-text")]//text()')

        for i in range(len(text)):
            if text[i][:4] == "pic." or text[i][:7] == "http://" or text[i][:4] == "www." or text[i][:8] == "https://":
                text[i] = " " + text[i]
            if text[i] == "\xa0" or text[i] == "…":
                text[i] = ""

        return "".join(text)
    except Exception as e:
        print(e)
        return ""


def query_expansion_tags(title, ownerId, start_date=None, end_date=None,
                         original=True, tag_min_frequency=2, ask_conf=True):
    """ applies query expansion related to tags """

    if end_date is None:
        end_date = datetime.datetime.now()

    if tag_min_frequency < 0 or tag_min_frequency is None:
        tag_min_frequency = 2

    time.sleep(2)

    ct = collection_item_count(title, ownerId, start_date, end_date)

    # current keywords set in the collection - to not suggest these ones
    current_keys = set(collection_keywords_list(title,ownerId))

    if ct > 0:

        list_facet_tags = __items_tags_facet_query(title, ownerId, start_date,
                                                   end_date, original)

        new_keywords = []

        i = 0

        if tag_min_frequency < 1:
            min_qtde = ct*tag_min_frequency
        else:
            min_qtde = math.pow(ct, 1/tag_min_frequency)

        while (i < len(list_facet_tags)
               and list_facet_tags[i]["count"] >= min_qtde):

            if ask_conf is False:
                new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    new_keywords.append(k)
            else:
                if list_facet_tags[i]["tags"] not in current_keys:
                    if str(input("add " + str(list_facet_tags[i]["tags"]) + "? (y/n)\n")) == "y":
                        new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    if k not in current_keys:
                        if str(input("add variant " + str(k) + "? (y/n)\n")) == "y":
                            new_keywords.append(k)

            i += 1

        collection_add_keywords(title, ownerId, new_keywords)
    else:
        print("Empty Collection")


def query_expansion_hashtags(title, ownerId, start_date=None, end_date=None,
                         original=True, hashtag_min_frequency=3, ask_conf=True):
    """ applies query expansion related to #hashtags only """

    if end_date is None:
        end_date = datetime.datetime.now()

    if hashtag_min_frequency < 0 or hashtag_min_frequency is None:
        hashtag_min_frequency = 3

    time.sleep(2)

    ct = collection_item_count(title, ownerId, start_date, end_date)

    # current keywords set in the collection - to not suggest these ones
    current_keys = set(collection_keywords_list(title,ownerId))

    if hashtag_min_frequency < 1:
        min_qtde = ct*hashtag_min_frequency
    else:
        min_qtde = math.pow(ct, 1/hashtag_min_frequency)

    hashtags_raw = dict()

    # extract the hashtags from the texts
    for it in __collectionContentGenerator(title, ownerId, start_date, end_date, original):
        for word in it["cleanTitle"].split(" "):
            if word.startswith("#"):
                if word in hashtags_raw.keys():
                    hashtags_raw[word] += 1
                else:
                    hashtags_raw[word] = 1

    hashtags = dict()

    # cleaning process
    for ht_raw in hashtags_raw.keys():

        # remove stresses
        ht = __strip_accents(ht_raw)

        # remove punctuation
        ht = ht.strip(string.punctuation + "…")

        # sum up redundancy
        if ht in hashtags.keys():
            hashtags[ht] += hashtags_raw[ht_raw]
        else:
            hashtags[ht] = hashtags_raw[ht_raw]

    new_keywords = []
    for ht in sorted(hashtags.items(), key = operator.itemgetter(1), reverse = True):
        if ht[1] < min_qtde:
            break
        if ask_conf is True and str(ht[0]) not in current_keys:
            print(type(ht))
            if str(input("add " + str(ht[0]) + "? (y/n)\n")) == "y":
                new_keywords.append(ht[0])
        else:
            new_keywords.append(ht[0])

    collection_add_keywords(title, ownerId, new_keywords)


def query_expansion_coocurrence_keywords(title, ownerId, start_date=None, end_date=None,
                         original=True, ask_conf=True):
    """ add keywords in pair, according to its co-occurence in the texts """

    if end_date is None:
        end_date = datetime.datetime.now()

    time.sleep(2)

    ct = 0

    current_keywords = set(collection_keywords_list(title, ownerId))
    coocur = dict()
    freq_k = dict()

    for it in __collectionContentGenerator(title, ownerId, start_date, end_date, original):
        ct += 1

        if 'tags' in it:
            tags_l = it['tags']
            tags_l.sort()

            # remove stresses
            tags_l = [__strip_accents(k) for k in tags_l]

            # remove punctuation
            tags_l = [k.strip(string.punctuation + "…") for k in tags_l]

            # to lower
            tags_l = [k.lower() for k in tags_l]

            tags_s = set(tags_l)

            for k in tags_s:
                if k in freq_k.keys():
                    freq_k[k] += 1
                else:
                    freq_k[k] = 1

            for pair in itertools.combinations(tags_l, 2):
                if pair[0] not in current_keywords and pair[1] not in current_keywords and str(pair[0] + " " + pair[1]) not in current_keywords and pair[0] < pair[1]:

                    if pair in coocur.keys():
                        coocur[pair] += 1
                    else:
                        coocur[pair] = 1

            for pair in coocur.keys():
                freq0 = freq_k[pair[0]]
                freq1 = freq_k[pair[1]]
                freq_p = coocur[pair]
                coocur[pair] = ((((freq0*freq1)**2)*freq_p)/(freq0+freq1))**(1/2)

    count_pair = 0
    new_keywords = []
    for it in sorted(coocur.items(), key = operator.itemgetter(1), reverse = True):
        count_pair += 1

        keyword = str(it[0][0] + " " + it[0][1])
        if count_pair > 20:
            break
        if ask_conf is True:
            if str(input("add " + keyword + "? (y/n)\n")) == "y":
                new_keywords.append(keyword)
        else:
            new_keywords.append(keyword)

    collection_add_keywords(title, ownerId, new_keywords)


def google_news_full_cover_urls(url, real_url=False):
    """ given the google news full coverage of an event, get the urls """

    page = 1
    links = set()
    length = 0

    urls = []
    if requests.get(url).url.startswith("https://news.google.com/"):
        while True:
            html = requests.get(url.format(page))
            soup = BeautifulSoup(html.content, "html.parser")
            links.update([a['href'] for a in soup.find_all('a', href=True)])

            if len(links) == length:
                break

            length = len(links)
            page += 1

        for link in sorted(links):
            if link.startswith("./article"):
                urls.append(link)
    else:
        print("Not a Google News url")

    Surls = set(urls)

    if real_url is False:
        return set(["https://news.google.com" + link[1:] for link in Surls])
    else:
        return set([requests.get("https://news.google.com" + link[1:]).url for link in Surls])

    return urls
