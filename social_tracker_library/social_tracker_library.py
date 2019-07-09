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
import signal
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

    try:
        a = Article(url=link)
        a.download()
        a.parse()
        a.fetch_images()

        for img in a.imgs:
            yield img
    except:
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


def __handler_timeout(signum, frame):
    print("timeout")
    raise Exception()


def url_media(csvlinks="link_list.csv", csvset="set_urls.csv",
              urldir="UrlMedia", medialog_file="medialog.json",
              directory="", ignore_twitter_link=True):

    root_dir = os.getcwd()

    if directory != "":
        os.chdir(directory)
        directory = os.getcwd()
    else:
        directory = root_dir

    setUrls = __csv_to_dict(csvset, 1, 0)

    if urldir[-1] == '/':
        urldir = urldir[:-1]

    seq = ""

    # get next sequence number
    if os.path.isfile(medialog_file):
        seq = __read_keyval_json("next_urlSeq", medialog_file)

    # if the parameter does not exist, get the seq from the
    if seq == "":
        seq = max([int(d) for d in os.listdir(urldir)] + [0]) + 1

    try:
        seqdir = os.path.realpath(urldir + "/" + str(seq))

        # implemented in order to give a feedback about progresss %
        total_row = sum(1 for row in __csvGenerator(csvlinks))
        row_count = 0

        # iterate through each link
        for line in __csvGenerator(csvlinks):
            row_count += 1

            if "https://twitter.com" in line[0] and ignore_twitter_link:
                continue

            url = __expandURL(line[0])

            if url not in setUrls.keys():

                print('\x1b[6;30;42m' + "Starting Scrapping for Link " +
                      str(url) + " (" + str(seq) + ")" + '\x1b[0m')

                os.mkdir(seqdir)
                os.chdir(seqdir)

                try:
                    # in order to avoid stalls in lives
                    signal.signal(signal.SIGALRM, __handler_timeout)
                    signal.alarm(1000)

                    youtube_dl.YoutubeDL({}).download([url])
                except KeyboardInterrupt as e:
                    raise
                except Exception as e:
                    print(e)
                finally:
                    signal.alarm(0)

                for im in __urlImageGenerator(url):
                    try:

                        if "base64," in im:
                            continue

                        lo = __lastocc(im,"/")+1

                        if lo < len(im)-1:
                            output = im[__lastocc(im,"/")+1:]
                        else:
                            output = im[__lastocc(im[:-1],"/")+1:-1]

                        if output == "" or len(output) > 80:
                            output = random.randint(1,10000000000000)

                        __request_download(link=im, output=str(output))
                    except requests.exceptions.ConnectionError as e:
                        print(e)
                        continue
                    except requests.exceptions.InvalidSchema as e:
                        print(e)
                        continue

                os.chdir(directory)

                setUrls[url] = seq

                __write_line_b_csv(csvfile=csvset, line=[seq, url])

                print('\x1b[6;30;42m' + "Scrap Finished for Link " + str(url) +
                      " (" + str(round(row_count*100/total_row, 4)) + "%)" + '\x1b[0m')

                seq += 1
                seqdir = os.path.realpath(urldir + "/" + str(seq))

        os.chdir(root_dir)

    except KeyboardInterrupt:
        print("Stopping...")

        __add_keyval_json("next_urlSeq", seq, medialog_file);

        os.chdir(root_dir)

        shutil.rmtree(seqdir)
    except Exception as e:
        __add_keyval_json("next_urlSeq", seq, medialog_file);

        os.chdir(root_dir)

        shutil.rmtree(seqdir)
        print(e)
        raise


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
    """ given the name of a collection, its owner and start/end date, returns
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
        with open(jsonfile, "r") as f:
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
