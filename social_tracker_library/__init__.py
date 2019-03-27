import stlib

# currently it is only a "mask"

def scrap_link(link, it, csvfile="links.csv"):
    stlib.scrap_link(link, it, csvfile)


def merge_media(csvimage="image.csv", csvvideo="video.csv",
                csvlinks="links.csv", csvitems="items.csv", imagepath="Images",
                videopath="Videos", linkspath="Links"):

    stlib.merge_media(csvimage, csvvideo, csvlinks, csvitems, imagepath,
                   videopath, linkspath)


def media_csv_download(csvfile, type_file="", directory="", csvset="",
                       from_beginning=False):
    stlib.media_csv_download(csvfile, type_file, directory. csvset, from_beginning)


def list_collections():
    stlib.list_collections()


def list_extracted_collections(path=None):
    stlib.list_extracted_collections(path)


def collection_item_count(title, ownerId, start_date=None, end_date=None,
                          original=True):
    stlib.collection_item_count(title, ownerId, start_date, end_date, original)


def extract_collection(title, ownerId, start_date=None, end_date=None):
    stlib.extract_collection(title, ownerId, start_date, end_date)


def create_collection(title, ownerId, keywords):
    stlib.create_collection(title, ownerId, keywords)


def collection_keywords_list(title, ownerId):
    return stlib.collection_keywords_list(title, ownerId)


def collection_add_keywords(title, ownerId, new_keywords):
    stlib.collection_add_keywords(title, ownerId, new_keywords)


def collection_remove_keywords(title, ownerId, keywords):
    stlib.collection_remove_keywords(title, ownerId, keywords)


def expand_texts(csvitems="items.csv", medialog_file="", from_beginning=False):
    stlib.expand_texts(csvitems, medialog_file, from_beginning)


def query_expansion(title, ownerId, type="tags", start_date=None, end_date=None,
                         original=True, rate=None, ask_conf=True):
    if type == "tags":
        stlib.query_expansion_tags(title, ownerId, start_date, end_date,
                                 original, rate, ask_conf)

    elif type == "hashtags":
        stlib.query_expansion_hashtags(title, ownerId, start_date, end_date,
                                 original, rate, ask_conf)

    elif type == "coocurrence":
        stlib.query_expansion_coocurrence_keywords(title, ownerId, start_date,
                                                end_date, original, ask_conf)


def google_news_full_cover_urls(url, real_url=False):
    stlib.google_news_full_cover_urls(url, real_url)
