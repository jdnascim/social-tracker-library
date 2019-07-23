import datetime
import csv
import os
import unicodedata
import glob
import json
from dateutil import parser


class Date:
    @classmethod
    def date2tmiles(cls, dat):
        """ return timemiles from a date at the same format used in the system
        """

        # TODO: require YYYY-MM-DD format

        return int(parser.parse(str(dat)).strftime("%s")) * 1000

    @classmethod
    def tmiles2date(cls, tmiles):
        """ return date from timemiles """
        return datetime.datetime.fromtimestamp(int(tmiles)/1.e3)

    @classmethod
    def now(cls, days=0):
        """ return now in miles """

        if days >= 0:
            return int(datetime.datetime.now().strftime("%s")
                       + datetime.timedelta(days=days)) * 1000
        else:
            return int(datetime.datetime.now().strftime("%s")
                       + datetime.timedelta(days=(-1)*days)) * 1000


class Text:
    @classmethod
    def strip_accents(cls, s):
        """ given a string, returns it without any stress """

        return ''.join([
            c for c in unicodedata.normalize('NFD', s) if unicodedata.category(
                c) != 'Mn']).lower()

    @classmethod
    def lastocc(cls, somestr, char):
        """ last occurence of a char in a string """

        if char in somestr:
            return max(i for i, c in enumerate(somestr) if c == char)
        else:
            return -1

    @classmethod
    def reduced_twitter_text(cls, text):
        """ returns True if the twitter item probably is shortened.
            Once the criterium was a little bit more complex, this
            is the reason why a nowadays quite simple function
            like this one was developed """

        # if "…http" in text.replace(" ", ""):

        if "…" in text:
            return True
        else:
            return False


class OSUtils:
    @classmethod
    def createDir(cls, path):
        """ creates directory and doesn't raise Exception in FileExists """
        try:
            os.mkdir(path)
        except FileExistsError:
            pass
        except Exception:
            raise

    @classmethod
    def list_to_file(cls, filename, listname):
        with open(filename, 'w') as filehandle:
            filehandle.writelines("%s\n" % i for i in listname)

    @classmethod
    def file_to_list(cls, filename):
        # define empty list
        tmp = []

        # open file and read the content in a list
        with open(filename, 'r') as filehandle:
            filecontents = filehandle.readlines()

            for line in filecontents:
                # remove linebreak which is the last character of the string
                tmp.append(line[:-1])

        return tmp

    @classmethod
    def str_to_file(cls, filename, s):
        with open(filename, 'w') as filehandle:
            filehandle.writelines("%s\n" % s)

    # This method is necessary, because youtube-dl adds extension
    # to the filename after downloading it. Plus, it may works
    # wrongly if a file with same name and different extension
    # is created after the one required. It means that you are
    # highly recommended use it just after downloading the media
    @classmethod
    def fileplusextension(cls, path):
        """ It returns path + .extension (e.g. im/im1 -> im/im1.jpeg) """

        most_recent_file = ""
        most_recent_time = 0

        for i in glob.glob(path+"*"):
            time = os.path.getmtime(i)
            if time > most_recent_time:
                most_recent_file = i
        return most_recent_file

    @classmethod
    def handler_timeout(cls, signum, frame):
        """ just a timeout handler :) """

        print("timeout")
        raise Exception()


class CSVUtils:
    @classmethod
    def write_line_b_csv(cls, csvfile, line, newfile=False):
        """ write a entire line into the csv file """

        if newfile is True and os.path.isfile(csvfile) is True:
            os.remove(csvfile)

        with open(csvfile, 'a') as resultfile:
            wr = csv.writer(resultfile, dialect='excel')
            wr.writerow(line)

    @classmethod
    def csvGenerator(cls, csvfile, delimiter=",", hide_header=True):
        """ csv generator """

        f = open(csvfile, "r")

        if hide_header is True:
            f.readline()

        for line in csv.reader(f, delimiter=delimiter):
            yield line

    @classmethod
    def csv_to_dict(cls, csvfile, id_key, id_value, delimiter=','):
        """ given a csv file, return a dict based upon it """

        csvgen = cls.csvGenerator(csvfile, delimiter=delimiter)

        csvdict = dict()

        for row in csvgen:
            csvdict[row[id_key]] = row[id_value]

        # TODO if every key is digit, turn into int
        # TODO id_value should be a list of index
        # TODO increment delimiter parameter (delimiter=',')

        return csvdict


class JSONUtils:
    @classmethod
    def add_keyval_json(cls, key, value, jsonfile):
        """ add a key-value to a json file """

        if os.path.isfile(jsonfile):
            with open(jsonfile, "r") as f:
                data = json.load(f)
        else:
            data = dict()

        data[key] = value

        with open(jsonfile, 'w') as f:
            json.dump(data, f, indent=2)

    @classmethod
    def read_keyval_json(cls, key, jsonfile):
        """ read a key-value of a json file  """

        if os.path.isfile(jsonfile):
            with open(jsonfile, "r") as f:
                data = json.load(f)
        else:
            return ""

        if key in data.keys():
            return data[key]
        else:
            return ""
