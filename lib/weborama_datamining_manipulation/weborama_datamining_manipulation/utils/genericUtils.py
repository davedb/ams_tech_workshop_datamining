# coding=utf-8

import datetime as datetime
import requests
import random
import os
import csv

#####################################
###        UNIX TIMESTAMP FX      ###
#####################################
def str2unixtime(date_str):
    """Returns a unix timestamp (int) from a string representing a date.

    :param date_str: str a string representing a date, formatted these ways:

    - DD/MM/YYYY
    - YYYY-MM-DD

    :rtype: an int representing a unix timestamp
    """
    strdate = date_str + " GMT +0000"
    if date_str.find('/') >= 0:
        strform = "%d/%m/%Y %Z %z"
    else:
        strform = "%Y-%m-%d %Z %z"

    return int(datetime.datetime.strptime(strdate,strform).timestamp())

def unixtime2datetime(unixtime):
    """Returns an object datetime.datetime from a unix timestamp.

    :param unixtime: int, a unix timestamp
    :rtype: datetime.datetime object
    """
    return datetime.datetime.fromtimestamp(int(unixtime))

#####################################
###   FROM DATE STR TO DATETIME   ###
#####################################

def string_2_datetime(string_date):
    """Transforms a string to datetime.datetime

    :param string_date: a str representing a date formatted these ways:

    - DD/MM/YYYY
    - YYYY-MM-DD

    :rtype: datetime.datetime object"""
    date_list = []
    if len(string_date.split('-')[0]) == 2:
        date_list = [int(string_date.split('-')[2]),int(string_date.split('-')[1]),int(string_date.split('-')[0])]
    else:
        date_list = [int(string_date.split('-')[0]),int(string_date.split('-')[1]),int(string_date.split('-')[2])]
    return datetime.datetime(date_list[0], date_list[1], date_list[2])


#####################################
###        HTTP REQUEST FX        ###
#####################################


def set_cookie_and_launch_url(affiche_w_string, url_wam):
    """Creates an *http* request, sets a cookie (checks if cookie id is 14 chars) and call the URL.

    :param affiche_w_string: str representing an affiche_w cookie id.
    :param url_wam: str representing the url to call (usually a wam url)
    :rtype: dict ``result = {'ok': 1/0,'status_code': 302/200/404 ...}``
    """

    result = {
        'ok' : 0,
        'status_code' : 0
    }
    # aggiorna il valore affiche_w da 12 a 14 caratteri per fare in modo che venga accettato il push o remove dalla piattaforma.
    if len(affiche_w_string) < 14:
        affiche_w_string = affiche_w_string + str(random.randint(0, 9)) + str(random.randint(0,9))

    cookies = dict(AFFICHE_W=affiche_w_string)
    try:
        content_request = requests.get(url_wam, cookies=cookies,allow_redirects=False,timeout=2)

        if content_request.status_code != 302 and content_request.status_code != 200:
            result['status_code'] = content_request.status_code
            # print("Bad status detected: {0}, affiche_w: {1}".format(str(content_request.status_code),affiche_w_string))
        else:
            result['ok'] = 1
            result['status_code'] = content_request.status_code
            # print("Cookie Action: code = {0}, affiche_w= {1}, url = {2}".format(str(content_request.status_code),affiche_w_string),url_wam)
    except:
        pass

    return result

#####################################
###        READ WRITE CSV         ###
#####################################

def save_list_to_csv(path_for_output, output_filename, element_list, header, csv_delimiter):
    """Saves a list to csv. every list inside ``element_list`` will be a row

    :param path_for_output: str path of the output
    :param output_filename: str name of the file
    :param element_list: list of lists: ``[['val1','val2','val3','val4'], ['val1','val2','val3','val4'] , ... ]``
    :param header: list ``['a','b','c','d']``
    :param csv_delimiter: str ``'\t' or ',' or ';'``
    """
    if os.path.exists(path_for_output) == False:
        os.makedirs(path_for_output)

    with open(os.path.join(path_for_output, output_filename), 'a+', encoding='utf-8', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter = csv_delimiter, quoting=csv.QUOTE_MINIMAL)

        if header[0] != '':
            spamwriter.writerow(header)

        for row in element_list:
            #print(row)
            spamwriter.writerow(row)

#####################################
### FIX AFFICHE_W FOR REINJECTION ###
#####################################

def fix_affiche_w(affiche_w_string):
    """Checks is  ``affiche_w_string`` is 14 chars long, if not add 2 random number

    :param affiche_w_string: str of affiche_w
    """
    if len(affiche_w_string) < 14:
        affiche_w_string = affiche_w_string + str(random.randint(0, 9)) + str(random.randint(0,9))
    return affiche_w_string
