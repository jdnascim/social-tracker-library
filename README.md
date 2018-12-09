# social-tracker-library
Python library to communicate with social tracker
(https://github.com/MKLab-ITI/mmdemo-dockerized)

Brief description of the methods:

(Obs: You are not supposed to call any method which name start with double
underline. Hence, these methods are not going to be cited here.)

list_collections()
    - It lists current collections in the system

create_collection(title, ownerId, keywords)
    - It creates an collection
    - It only set keywords, setting users is still to be developed.
    - Keywords parameter should be an list

collection_item_count(title, ownerId, start_date=None, end_date=None,
                      original=True)
    - It returns the number of items related to the collections.
    - Dates indicates a range of time which an item should be posted in order
    to be counted. If None, start_date will be the collection start date, which
    is usually 30 days before its creation, and there will not be an end date.
    - Parameter "original" indicates if, for instance, retweeted items should be
    counted

collection_add_keywords(title, ownerId, new_keywords)
    - add new keywords to a given collection

query_expansion_tags(title, ownerId, tag_min_frequency=0.05,
        start_date=None, end_date=None, original=True)
    - Query expansion. It executes a facet query, and then the terms which
    the frequency is higher that the one passed as parameter is added as
    keywords
    - Dates and original parameters: they are explained in the description of
    the method collection_item_count()

Items Extraction: In order to extract items from the database of the system to
generate a dataset with text, images and videos, the following methods should
be executed, in the following order:

1 - extract_collection(title, ownerId, start_date=None, end_date=None)
    - It extracts the items of a given collection.
    - Dates parameters: they are explained in the description of
    the method collection_item_count().
    - Currently, only original items are extracted.

2 - media_csv_download(csvfile, type_file="", directory=".")
    - Given the csv file, which contains social media items that have images or
    videos related to, try to download these images or Videos
    - You should call twice, once with the parameter csvfile as "image.csv",
    and once with the parameter csvfile as "video.csv".
    - You should specify the type_file (image or video) if the csvfile is not
    named image.csv or video.csv
    - The directory that the media is saved is directory/image and
    directory/video (it depends on the type)


3 - scrap_link(link, it, csvfile="links.csv")
    - Given all links which is in the text of the items, it tries to scrap
    images and videos from these links.
    - Unstable

4 - merge_media(csvimage="image.csv", csvvideo="video.csv",
            csvlinks="links.csv", csvitems="items.csv", imagepath="Images",
            videopath="Videos", linkspath="Links")
    - Merge items collected in the step 2 and 3 (and delete redundant images or
    videos)
    - Unstable

Any doubts, feel free to contact me by the email jose.dori.nascimento@gmail.com
