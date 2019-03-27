import social_tracker as st

# currently it is only a "mask"

def scrap_link(link, it, csvfile="links.csv"):
    st.scrap_link(link, it, csvfile)


def merge_media(csvimage="image.csv", csvvideo="video.csv",
                csvlinks="links.csv", csvitems="items.csv", imagepath="Images",
                videopath="Videos", linkspath="Links"):

    st.merge_media(csvimage, csvvideo, csvlinks, csvitems, imagepath,
                   videopath, linkspath)


def media_csv_download(csvfile, type_file="", directory="", csvset="",
                       from_beginning=False):
    st.media_csv_download(csvfile, type_file, directory. csvset, from_beginning)


def list_collections():
    st.list_collections()


def list_extracted_collections(path=None):
    st.list_extracted_collections(path)


def collection_item_count(title, ownerId, start_date=None, end_date=None,
                          original=True):
    st.collection_item_count(title, ownerId, start_date, end_date, original)


def extract_collection(title, ownerId, start_date=None, end_date=None):
    st.extract_collection(title, ownerId, start_date, end_date)


def create_collection(title, ownerId, keywords):
    st.create_collection(title, ownerId, keywords)


def collection_keywords_list(title, ownerId):
    return st.collection_keywords_list(title, ownerId)


def collection_add_keywords(title, ownerId, new_keywords):
    st.collection_add_keywords(title, ownerId, new_keywords)


def collection_remove_keywords(title, ownerId, keywords):
    st.collection_remove_keywords(title, ownerId, keywords)


def expand_texts(csvitems="items.csv", medialog_file="", from_beginning=False):
    st.expand_texts(csvitems, medialog_file, from_beginning)


def query_expansion(title, ownerId, type="tags", start_date=None, end_date=None,
                         original=True, rate=None, ask_conf=True):
    if type == "tags":
        st.query_expansion_tags(title, ownerId, start_date, end_date,
                                 original, rate, ask_conf)

    elif type == "hashtags":
        st.query_expansion_hashtags(title, ownerId, start_date, end_date,
                                 original, rate, ask_conf)

    elif type == "coocurrence":
        st.query_expansion_coocurrence_keywords(title, ownerId, start_date,
                                                end_date, original, ask_conf):


def google_news_full_cover_urls(url, real_url=False):
    st.google_news_full_cover_urls(url=, real_url)
