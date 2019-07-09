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
            # if from beginning is false, execute a previous loop, which
            # tries to skip to the last image downloaded (therefore it
            # will not check if each image exists or not, and will enhance
            # performance)
            if from_beginning is False:
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

            # Image Loop
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
            # Previous Loop (See explanation in the image case)
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

            # Video Loop
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

def extract_collection(title, ownerId, start_date=None, end_date=None):
    """ extracts items from a given collection, and organizes it in files """

    # creates dir where things are going to be stored
    directory = str(title) + "_" + str(ownerId)

    directory = directory.replace(" ", "")

    __createDir(directory)
    __createDir(directory + "/Images")
    __createDir(directory + "/Videos")
    __createDir(directory + "/Media_URL")


    # header of the csv files
    head = ["csvid", "text", "location", "pubtime", "tags", "media", "source", "itemUrl"]
    head_media = ["name", "csvid", "text", "location", "pubtime", "tags",
                  "source", "url"]

    head_list = ["link", "csvid_dir"]

    head_set_links = ["link", "item_id", "path"]

    head_set_urls = ["seq", "url"]

    # writes the headers
    __write_line_b_csv(directory + "/items.csv", head, newfile=True)
    __write_line_b_csv(directory + "/image.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/video.csv", head_media, newfile=True)
    __write_line_b_csv(directory + "/link_list.csv", head_list, newfile=True)
    __write_line_b_csv(directory + "/set_links.csv", head_set_links,
                       newfile=True)
    __write_line_b_csv(directory + "/set_urls.csv", head_set_links,
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
