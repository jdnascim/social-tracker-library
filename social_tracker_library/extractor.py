import os
import filetype
import csv
import shutil
import requests
import youtube_dl
import random
import signal
from newspaper import Article
from lxml import html

from .constants import CSVITEMS, CSVIMAGE, CSVVIDEO, IMAGEDIR, VIDEODIR
from .constants import URLDIR, CSVLINKS, CSVSETLINKS, CSVSETURL, MEDIALOG
from .constants import CONF_JSON
from .utils import CSVUtils, OSUtils, JSONUtils, Text


class extractor:
    conf_json = CONF_JSON

    def __init__(self, directory=None):
        base = JSONUtils.read_keyval_json("COLLECTIONS",
                                          self.conf_json)["path"]
        if directory is None:
            self.directory = base
        elif os.path.isdir(directory) is True:
            self.directory = directory
        elif os.path.isdir(base + directory):
            self.directory = base + directory
        else:
            raise Exception("Collection does not exist")

    @classmethod
    def __expandURL(self, link):
        """ returns "true" link to a page """
        try:
            return requests.get(link).url
        except Exception:
            return link

    @classmethod
    def __full_tweet_text(cls, link_t):
        """ given a link to a tweet, extract its entire text. It is necessary
        for twitter items which has more than 140 characters """

        try:
            page = requests.get(link_t)
            tree = html.fromstring(page.content)
            text = tree.xpath('//div[contains(@class, \
                              "permalink-tweet-container")]//p[contains(@class,\
                               "tweet-text")]//text()')

            for i in range(len(text)):
                if (text[i][:4] == "pic.") or (
                    text[i][:7] == "http://") or (
                        text[i][:4] == "www.") or (text[i][:8] == "https://"):

                    text[i] = " " + text[i]
                if text[i] == "\xa0" or text[i] == "…":
                    text[i] = ""

            return "".join(text)
        except Exception as e:
            print(e)
            return ""

    @classmethod
    def __urlImageGenerator(cls, link):
        """ given a link, try to get images from it by the Article Library
        """

        try:
            a = Article(url=link)
            a.download()
            a.parse()
            a.fetch_images()

            for img in a.imgs:
                yield img
        except Exception:
            pass

    def __request_download(self, link, output, overwrite=False):
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
            if os.path.isfile(OSUtils.fileplusextension(output)):
                os.remove(OSUtils.fileplusextension(output))
            raise
        except Exception:
            raise

    def __youtube_download(self, link, output=None, noplaylist=True,
                           overwrite=False):
        """ youtube download """

        try:
            if os.path.isfile(output) is False or overwrite is True:
                youtube_dl.YoutubeDL({'outtmpl': output,
                                      'noplaylist': True,
                                      'socket_timeout': 60}).download([link])

                shutil.move(OSUtils.fileplusextension(output), output)

                if bool(filetype.guess_mime(output)) is True:
                    print(link, output)
                    return True
                else:
                    return False
            else:
                print("File " + output + " exists.")
                return False
        except KeyboardInterrupt:
            if os.path.isfile(OSUtils.fileplusextension(output)):
                os.remove(OSUtils.fileplusextension(output))
            raise
        except Exception:
            pass

    def media_csv_download(self, type_file="", csvfile="", csvset="",
                           medialog_file="", from_beginning=False,
                           sample=False, overwrite=False):
        """ download media from the csv generated
            type parameter: (I)mage or (V)ideo
            from_beginning: defines if it will start based on the medialog_file
            sample: if it is a number, download (randomly and approximately)
            only total*sample items """

        # if type_file not specified, download images AND videos
        # it will recurse only once
        if type_file == "" or type_file is None:
            self.media_csv_download("i", csvfile, csvset, medialog_file,
                                    from_beginning, sample, overwrite)
            self.media_csv_download("v", csvfile, csvset, medialog_file,
                                    from_beginning, sample, overwrite)
            return ""

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

        type_file = type_file.lower()

        if type_file not in valid_types:
            raise ValueError("Parameter type_file/csvfile must be one of %r." %
                             valid_types)

        # if csvfile was passed as parameter, assumes the directory as
        # the same of the csvfile
        if csvfile != "":
            directory = csvfile[:Text.lastocc(csvfile, "/")]
            csvfile = csvfile[Text.lastocc(csvfile, "/")+1:]
        else:
            if type_file == "i" or type_file == "image":
                csvfile = CSVIMAGE
            else:
                csvfile = CSVVIDEO

            if self.directory[-1] == "/":
                directory = self.directory[:-1]
            else:
                directory = self.directory

        if csvset == "":
            csvset = self.directory + "/" + CSVSETLINKS

        if medialog_file == "":
            medialog_file = self.directory + "/" + MEDIALOG

        # now it iterates through the csvfile [csvGenerator()] and download the
        # items [*.download() -> depends on the type specified]

        csvGen = CSVUtils.csvGenerator(directory + "/" + csvfile)

        # if from_beginning = False, try to start from the file where it
        # stopped in the former iteration
        last_image = str(JSONUtils.read_keyval_json("last_image",
                                                    medialog_file))
        last_video = str(JSONUtils.read_keyval_json("last_video",
                                                    medialog_file))

        try:
            if type_file == "i" or type_file == "image":
                # if from beginning is false, execute a previous loop, which
                # tries to skip to the last image downloaded (therefore it
                # will not check if each image exists or not, and will enhance
                # performance)

                # percentage
                n_downloaded = 1
                qtde_items = CSVUtils.count_lines(directory + "/" + CSVIMAGE)

                if from_beginning is False:
                    if last_image != "":
                        last_image_fl = float(last_image.replace("_", "."))
                    else:
                        last_image = "0_0"
                        last_image_fl = 0.0

                    if last_image_fl > 0.0:
                        for line in csvGen:
                            if float(str(line[0]).replace("_", "."
                                                          )) == last_image_fl:
                                print("Skipping until", line[0])
                                n_downloaded += 1
                                break
                            elif float(str(line[0]).replace("_", "."
                                                            )) > last_image_fl:
                                print(float(str(line[0]).replace(
                                    "_", ".")), last_image_fl)
                                print("Error: Last image in log does not \
                                      exist - Starting from the beginning")
                                n_downloaded = 1
                                csvGen = CSVUtils.csvGenerator(directory + "/"
                                                               + csvfile)
                                break
                            else:
                                n_downloaded += 1

                # Image Loop
                for line in csvGen:
                    linkfile = directory + "/" + IMAGEDIR + line[0]

                    try:
                        # if sample mode is set, download only a small number
                        # of items, based on the sample probability
                        if sample_mode is True:
                            if random.random() < sample is False:
                                continue

                        chk = self.__request_download(line[7], linkfile,
                                                      overwrite=overwrite)

                        print('\x1b[6;30;42m'
                              + str(round(n_downloaded*100/qtde_items, 4))
                              + "%" + '\x1b[0m')
                        n_downloaded += 1

                        if chk is True:
                            CSVUtils.write_line_b_csv(csvset,
                                                      [self.__expandURL(
                                                          line[7]),
                                                       line[0], linkfile])
                            last_image = line[0]
                    except requests.exceptions.ConnectionError:
                        continue

                # set the last image downloaded after the end of the loop
                if sample_mode is False:
                    JSONUtils.add_keyval_json("last_image", last_image,
                                              medialog_file)

            elif type_file == "v" or type_file == "video":
                # percentage
                n_downloaded = 1
                qtde_items = CSVUtils.count_lines(directory + "/" + CSVVIDEO)

                # Previous Loop (See explanation in the image case)
                if from_beginning is False:
                    if last_video != "":
                        last_video_fl = float(last_video.replace("_", "."))
                    else:
                        last_video = "0_0"
                        last_video_fl = 0.0

                    if last_video_fl > 0.0:
                        for line in csvGen:
                            if float(str(line[0]
                                         ).replace("_", ".")) == last_video_fl:
                                print("Skipping", line[0])
                                n_downloaded += 1
                                break
                            elif float(str(line[0]).replace("_", "."
                                                            )) > last_video_fl:
                                print("Error: Last video in log does not exist\
                                       - Starting from the beginning")
                                n_downloaded = 1
                                csvGen = CSVUtils.csvGenerator(directory + "/"
                                                               + csvfile)
                                break
                            else:
                                n_downloaded += 1

                # Video Loop
                for line in csvGen:
                    # if sample mode is set, download only a small number of
                    # items, based on the sample probability
                    if sample_mode is True:
                        if random.random() < sample is False:
                            continue

                    linkfile = directory + "/" + VIDEODIR + line[0]

                    chk = self.__youtube_download(line[7], linkfile,
                                                  overwrite=overwrite)

                    print('\x1b[6;30;42m'
                          + str(round(n_downloaded*100/qtde_items, 4))
                          + "%" + '\x1b[0m')
                    n_downloaded += 1

                    if chk is True:
                        CSVUtils.write_line_b_csv(csvset,
                                                  [self.__expandURL(line[7]),
                                                   line[1], linkfile])

                        last_video = line[0]

                # set the last video downloaded after the end of the loop
                if sample_mode is False:
                    JSONUtils.add_keyval_json("last_video", last_video,
                                              medialog_file)
        except KeyboardInterrupt:
            print("\nStopping...")

            # set last item succesfully downloaded in the medialog file:
            if sample_mode is False:
                if type_file == "i" or type_file == "image":
                    JSONUtils.add_keyval_json("last_image", last_image,
                                              medialog_file)
                elif type_file == "v" or type_file == "video":
                    JSONUtils.add_keyval_json("last_video", last_video,
                                              medialog_file)
        except Exception as e:
            print(e)
            raise

    @classmethod
    def list_extracted_collections(cls, path=None):
        """ lists every extracted collection in the directory
            configured in COLLECTIONS_DIR """

        print("List of the extracted collections")
        print()

        cols = []

        if path is None:
            conf = JSONUtils.read_keyval_json("COLLECTIONS", cls.conf_json)

            path = conf["path"]

        try:
            dir_list = next(os.walk(path))[1]
            for dire in dir_list:
                if dire[0] != '.':
                    cols.append(dire)
                    print(dire)

            return cols
        except Exception:
            pass

    def expand_texts(self, csvitems="", medialog_file="",
                     from_beginning=False):
        """ Sometimes, a Twitter item's text is not entirely stored in the
            database. This function aims to scrap the tweet url in order to
            get the full text """

        if csvitems == "":
            csvitems = self.directory + "/" + CSVITEMS

        if medialog_file == "":
            medialog_file = self.directory + "/" + MEDIALOG

        # PRE: determine directory and name of the augmented texts's file
        last_dot = Text.lastocc(csvitems, ".")

        if last_dot == -1:
            last_dot = len(csvitems)

        AUG_IT = csvitems[:last_dot] + "_AUG_TEXT" + csvitems[last_dot:]

        # write the header in the new file
        itemsgen = CSVUtils.csvGenerator(csvitems)

        # reach the items generator to the start point (according to whether it
        # should start from the beginning or not)
        if from_beginning is False and os.path.isfile(AUG_IT) is True:
            last_text = JSONUtils.read_keyval_json("last_text", medialog_file)

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
                    itemsgen = CSVUtils.csvGenerator(csvitems)
                    next(itemsgen)
            else:
                last_text = 0
        else:
            # write header of csvitem into csvitem_AUG
            CSVUtils.write_line_b_csv(AUG_IT,
                                      next(csv.reader(open(csvitems, "r"))),
                                      True)
            last_text = 0

        # for each item, verify if its text should be expanded
        try:
            # TODO - Add %
            for it in itemsgen:
                if Text.reduced_twitter_text(it[1]):
                    full_text = self.__full_tweet_text(it[8])

                    if full_text != "":
                        # write the expanded text
                        CSVUtils.write_line_b_csv(AUG_IT,
                                                  it[:1] + [full_text] + it[2:]
                                                  )
                        print(it[0], full_text)
                    else:
                        CSVUtils.write_line_b_csv(AUG_IT, it)
                        print(it[0], it[1])
                else:
                    CSVUtils.write_line_b_csv(AUG_IT, it)

                last_text = it[0]

            # set the last text after finish the loop.
            JSONUtils.add_keyval_json("last_text", last_text, medialog_file)
            print("Finished")
        except KeyboardInterrupt:
            print("\nStopping...")

            # set last verified text in the medialog file:
            JSONUtils.add_keyval_json("last_text", last_text, medialog_file)
        except Exception as e:
            print(e)
            JSONUtils.add_keyval_json("last_text", last_text, medialog_file)
            raise

    def url_media(self, csvlinks="", csvset="", urldir="", medialog_file="",
                  directory="", ignore_twitter_link=True, mediatype="vi",
                  site_sources=[], name_scraping="", video_timelimit=1000):
        """ scrap links from link_list """

        if csvlinks == "":
            csvlinks = CSVLINKS
        if csvset == "":
            csvset = CSVSETURL
        if medialog_file == "":
            medialog_file = MEDIALOG
        if directory == "":
            directory = self.directory

        if urldir == "" and name_scraping == "":
            urldir = URLDIR
            name_scraping = urldir.lower()
        elif name_scraping == "":
            name_scraping = urldir.lower()
        elif urldir == "":
            urldir = name_scraping

        if urldir[-1] != "/":
            urldir = urldir + "/"
        if name_scraping[-1] == "/":
            name_scraping = name_scraping[:-1]

        mediatype = str(mediatype).lower()
        if mediatype not in ("v", "i", "vi", "iv"):
            mediatype = "vi"

        root_dir = os.getcwd()

        if directory != "":
            os.chdir(directory)
            directory = os.getcwd()
        else:
            directory = root_dir

        setUrls = CSVUtils.csv_to_dict(csvset, 1, 0)

        if urldir[-1] == '/':
            urldir = urldir[:-1]
        OSUtils.createDir(urldir)

        seq = ""

        # get next sequence number
        if os.path.isfile(medialog_file):
            seq = JSONUtils.read_keyval_json("next_"+name_scraping+"Seq",
                                             medialog_file)

        # if the parameter does not exist, get the seq from the
        if seq == "":
            seq = max([int(d) for d in os.listdir(urldir)] + [0]) + 1

        try:
            seqdir = os.path.realpath(urldir + "/" + str(seq))

            # implemented in order to give a feedback about progresss %
            total_row = sum(1 for row in CSVUtils.csvGenerator(csvlinks))
            row_count = 0

            # iterate through each link
            for line in CSVUtils.csvGenerator(csvlinks):
                row_count += 1

                if "https://twitter.com" in line[0] and ignore_twitter_link:
                    continue

                url = self.__expandURL(line[0])

                if len(site_sources) > 0:
                    if len([site for site in site_sources if site in url]
                           ) == 0:
                        continue

                if url not in setUrls.keys():

                    print('\x1b[6;30;42m' + "Starting Scrapping for Link "
                          + str(url) + " (" + str(seq) + ")" + '\x1b[0m')

                    os.mkdir(seqdir)
                    os.chdir(seqdir)

                    if "v" in mediatype:
                        try:
                            # in order to avoid stalls in lives
                            signal.signal(signal.SIGALRM,
                                          OSUtils.handler_timeout)
                            signal.alarm(video_timelimit)

                            youtube_dl.YoutubeDL({}).download([url])
                        except KeyboardInterrupt:
                            raise
                        except Exception as e:
                            print(e)
                        finally:
                            signal.alarm(0)

                    if "i" in mediatype:
                        for im in self.__urlImageGenerator(url):
                            try:
                                signal.signal(signal.SIGALRM,
                                              OSUtils.handler_timeout)
                                signal.alarm(video_timelimit)

                                if "base64," in im:
                                    continue

                                lo = Text.lastocc(im, "/")+1

                                if lo < len(im)-1:
                                    output = im[Text.lastocc(im, "/")+1:]
                                else:
                                    output = im[
                                        Text.lastocc(im[:-1], "/")+1:-1]

                                if output == "" or len(output) > 80:
                                    output = random.randint(1, 10000000000000)

                                self.__request_download(link=im,
                                                        output=str(output))
                            except requests.exceptions.ConnectionError as e:
                                print(e)
                                continue
                            except requests.exceptions.InvalidSchema as e:
                                print(e)
                                continue
                            finally:
                                signal.alarm(0)

                    os.chdir(directory)

                    setUrls[url] = seq

                    CSVUtils.write_line_b_csv(csvfile=csvset, line=[seq, url])

                    print('\x1b[6;30;42m' + "Scrap Finished for Link "
                          + str(url) + " ("
                          + str(round(row_count*100/total_row, 4)) + "%)"
                          + '\x1b[0m')

                    seq += 1
                    seqdir = os.path.realpath(urldir + "/" + str(seq))

            os.chdir(root_dir)

        except KeyboardInterrupt:
            print("Stopping...")

            JSONUtils.add_keyval_json("next_"+name_scraping+"Seq", seq,
                                      medialog_file)

            os.chdir(root_dir)

            shutil.rmtree(seqdir)
        except Exception as e:
            JSONUtils.add_keyval_json("next_"+name_scraping+"Seq", seq,
                                      medialog_file)

            os.chdir(root_dir)

            shutil.rmtree(seqdir)
            print(e)
            raise
