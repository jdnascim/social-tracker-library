import datetime
import math
import time
import itertools
import string
import os
import copy
import operator

from .collection import collection
from .utils import Text, OSUtils
from .constants import QE_STOPWORDS, QE_PLACES, QE_PLACES_DIR, QE_LOG


class query_expansion:
    def __init__(self, col: collection):
        self.col = col
        self.stopwords = set(OSUtils.file_to_list(QE_STOPWORDS))

    def places_available():
        for s in os.listdir(QE_PLACES):
            print(s[:-4])

    def __qe_col_copy(self):
        # for integrity reasons, col should have an end_date. If the original
        # end_date is None, end_date is set to the current date, and then as
        # None at the end of this method
        col = copy.deepcopy(self.col)
        if col.end_date is None:
            col.end_date = datetime.datetime.now()

        return col

    def __pair_in_keywords(self, key1, key2):
        """ verifies if a pair is among the collection keywords """

        current_keywords = self.col.keywords_list
        if key1 in current_keywords:
            return False
        if key2 in current_keywords:
            return False
        if str(key1 + " " + key2) in current_keywords:
            return False
        if key1 < key2:
            return False

        return True

    def __write_qe_log(title, ownerId, key_initial, key_after):
        log = str(title) + '\n'
        log += str(ownerId) + '\n'
        log += "Initial Keywords: " + str(key_initial) + '\n'
        log += "Final Keywords: " + str(key_after) + '\n\n'

        OSUtils.str_to_file(QE_LOG, log)

    def Tags(self, tag_min_frequency=2, limit_suggestion=20,
             stopwords_analysis=True, places_analysis='BR', ask_conf=True,
             log=True, add=True):
        """ applies query expansion related to tags """

        col = self.__qe_col_copy()

        # verify integrity of the rate
        if tag_min_frequency < 0 or (tag_min_frequency is None
                                     and limit_suggestion is None):
            raise Exception("invalid frequency: " + str(tag_min_frequency)
                            + " - it should be bigger than 0")

        # places stopwords analysis
        if places_analysis is not None and places_analysis in QE_PLACES:
            places = set(OSUtils.file_to_list(QE_PLACES_DIR + QE_PLACES
                                              + ".txt"))

        # brief sleep for integrity reasons
        time.sleep(2)

        # collection's qtde of items
        ct = col.item_count()

        # collection's current keywords - in order to not suggest these ones
        current_keys = set(col.keywords_list())

        if ct <= 0:
            raise Exception("Empty Collection")

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
        while (i < len(list_facet_tags) and i < limit_suggestion
               and list_facet_tags[i]["count"] >= min_qtde):

            if counter > limit_suggestion:
                break

            if stopwords_analysis is True and (list_facet_tags[i]["tags"]
                                               in self.stopwords):

                i += 1
                continue

            if places_analysis is not None and (list_facet_tags[i]['tags']
                                                in places):

                i += 1
                continue

            # if ask_conf is false, just add. Otherwise, ask first
            if ask_conf is False:
                new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    new_keywords.append(k)
            else:
                if list_facet_tags[i]["tags"] not in current_keys and str(
                    input("add " + str(list_facet_tags[i]["tags"])
                          + "? (y/n)\n")) == "y":
                        new_keywords.append(list_facet_tags[i]["tags"])

                for k in list_facet_tags[i]["variant_keys"]:
                    if k not in current_keys and str(
                        input("add variant " + str(k)
                              + "? (y/n)\n")) == "y":
                            new_keywords.append(k)

            i += 1
            counter += 1

        if add is True:
            col.add_keywords(new_keywords)

        if log is True:
            self.__write_qe_log(col.title, col.ownerId, current_keys,
                                col.keywords_list())

        return new_keywords

    def __coocurrence_formula(self, freq0, freq1, freq_p):
        """ co-occurence formula """
        return ((((freq0*freq1)**2)*freq_p)/(freq0+freq1))**(1/2)

    def Cooccurrence(self, limit_suggestion=20, ask_conf=True, add=True,
                     log=True):
        """ add keywords in pair, according to its co-occurence in the texts
        """

        # TODO: Rewrite this to limit the co-occurence pairs tested - if there
        # is too many items, the method takes a lot of time to conclude

        col = self.__qe_col_copy()

        time.sleep(2)

        ct = 0

        coocur = dict()
        freq_k = dict()

        current_keys = col.keywords_list()

        for it in self.col.ContentGenerator():
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
                    if self.__pair_in_keywords(pair[0], pair[1]):

                        if pair in coocur.keys():
                            coocur[pair] += 1
                        else:
                            coocur[pair] = 1

                for pair in coocur.keys():
                    freq0 = freq_k[pair[0]]
                    freq1 = freq_k[pair[1]]
                    freq_p = coocur[pair]

                    # Co-occurrence Formula!!!!!
                    coocur[pair] = self.__coocurrence_formula(freq0, freq1,
                                                              freq_p)

        count_pair = 0
        new_keywords = []
        for it in sorted(coocur.items(), key=operator.itemgetter(1),
                         reverse=True):
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

        if log is True:
            self.__write_qe_log(col.title, col.ownerId, current_keys,
                                col.keywords_list())
