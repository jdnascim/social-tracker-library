# put constants here

import os

QE_LOG = os.path.dirname(os.path.realpath(__file__)) + "/qe_sets/qe_log.txt"

QE_STOPWORDS_DIR = os.path.dirname(os.path.realpath(__file__)
                                   ) + "/qe_sets/stopwords/"

QE_PLACES_DIR = os.path.dirname(os.path.realpath(__file__)
                                ) + "/qe_sets/places/"

QE_EVENT_THESAURUS_DIR = os.path.dirname(os.path.realpath(__file__)
                                         ) + "/qe_sets/event_thesaurus/"

CSVITEMS = "items.csv"

CSVIMAGE = "image.csv"

CSVVIDEO = "video.csv"

CSVLINKS = "link_list.csv"

CSVSETLINKS = "set_links.csv"

CSVSETURL = "set_urls.csv"

MEDIALOG = "medialog.json"

IMAGEDIR = "Images/"

VIDEODIR = "Videos/"

URLDIR = "UrlMedia/"

HEAD_ITEMS = ["csvid", "text", "location", "pubtime", "tags", "media",
              "source", "itemUrl"]

HEAD_MEDIA = ["name", "csvid", "text", "location", "pubtime", "tags",
              "source", "url"]

HEAD_LINK_LIST = ["link", "csvid_dir"]

HEAD_SET_LINKS = ["link", "item_id", "path"]

HEAD_SET_URLS = ["seq", "url"]

CONF_JSON = "conf.json"
