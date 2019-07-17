import datetime
import math
import time
import itertools
import string
import os

from .collection import collection
from .utils import Text, OSUtils
from .constants import QE_STOPWORDS, QE_PLACES, QE_PLACES_DIR, QE_EVENT_THESAURUS_DIR

class query_expansion:
    def __init__(self, col: collection):
        self.col = col
        stopwords = set(OSUtils.file_to_list(QE_STOPWORDS))

    def places_available():
        for s in os.listdir(QE_PLACES):
            print(s[:-4])

    def Tags(self, tag_min_frequency=2, ask_conf=True, limit_suggestion=20,
             stopwords_analysis=True, places_analysis='BR'):
        """ applies query expansion related to tags """

        # for integrity reasons, col should have an end_date. If the original
        # end_date is None, end_date is set to the current date, and then as
        # None at the end of this method
        end_date_is_none = False
        if self.col.end_date is None:
            self.col.end_date = datetime.datetime.now()
            end_date_is_none = True

        # verify integrity of the rate
        if tag_min_frequency < 0 or (tag_min_frequency is None and limit_suggestion is None):
            raise Exception("invalid frequency: " + str(tag_min_frequency) +
                            " - it should be bigger than 0")

        # places stopwords analysis
        if places_analysis is not None and places_analysis in QE_PLACES:
            places = set(OSUtils.file_to_list(QE_PLACES_DIR + QE_PLACES + ".txt"))

        # brief sleep for integrity reasons
        time.sleep(2)

        # collection's qtde of items
        ct = col.item_count()

        # collection's current keywords - in order to not suggest these ones
        current_keys = set(col.keywords_list())

        if ct > 0:

            # facet query execution
            list_facet_tags = col.tags_facet_query()

            new_keywords = []

            # index through the facet query list
            i = 0

            # counts how many tags were suggested
            counter = 0

            # based on the frequency, counts the minumum appearance required
            # for a tags to be suggested
            if tag_min_frequency < 1:
                min_qtde = ct*tag_min_frequency
            else:
                min_qtde = math.pow(ct, 1/tag_min_frequency)

            # iteration through the tags list
            while (i < len(list_facet_tags) and i < max_suggestion
                   and list_facet_tags[i]["count"] >= min_qtde):

                if counter > limit_suggestion:
                    break

                if stopwords_analysis is True and list_facet_tags[i]["tags"] in stopwords:

                    i += 1
                    continue

                if places_analysis is not None and list_facet_tags[i]['tags'] in places:

                    i += 1
                    continue

                # if ask_conf is false, just add. Otherwise, ask first
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
                counter += 1

            col.add_keywords(new_keywords)
        else:
            print("Empty Collection")

        # returns its original value
        if end_date_is_none is True:
            col.end_date = None


    def Cooccurrence(self, ask_conf=True, limit_suggestion=20):
        """ add keywords in pair, according to its co-occurence in the texts """

        # for integrity reasons, col should have an end_date. If the original
        # end_date is None, end_date is set to the current date, and then as
        # None at the end of this method
        end_date_is_none = False
        if self.col.end_date is None:
            self.col.end_date = datetime.datetime.now()
            end_date_is_none = True

        time.sleep(2)

        ct = 0

        current_keywords = set(col.keywords_list())
        coocur = dict()
        freq_k = dict()

        for it in col.ContentGenerator():
            ct += 1

            if 'tags' in it:
                tags_l = it['tags']
                tags_l.sort()

                # remove stresses
                tags_l = [Text.strip_accents(k) for k in tags_l]

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

                    ##### Co-occurrence Formula #####
                    coocur[pair] = ((((freq0*freq1)**2)*freq_p)/(freq0+freq1))**(1/2)

        count_pair = 0
        new_keywords = []
        for it in sorted(coocur.items(), key = operator.itemgetter(1), reverse = True):
            count_pair += 1

            keyword = str(it[0][0] + " " + it[0][1])
            if count_pair > limit_suggestion:
                break
            if ask_conf is True:
                if str(input("add " + keyword + "? (y/n)\n")) == "y":
                    new_keywords.append(keyword)
            else:
                new_keywords.append(keyword)

        col.add_keywords(new_keywords)

        # returns its original value
        if end_date_is_none is True:
            col.end_date = None
