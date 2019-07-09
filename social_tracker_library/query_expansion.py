import datetime
import math
import time
import itertools

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
