import os
import sys
import random

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), 'lib/weborama_datamining_manipulation/weborama_datamining_manipulation'))

from utils.awsManager import AwsManager
from utils.gzipManagerBase import GzipManagerBase
from datamining.dmAnalyzer import DmAnalyzer
from utils.genericUtils import fix_affiche_w
from utils.genericUtils import save_list_to_csv

from helper import Helper


class Main:
    TOP_SPENDER_CUSTOM_SEGMENT_ID = 130130500

    VIDEO_VIEWER_AUDIENCE_PLUS = 654845
    VIDEO_VIEWER_AUDIENCE_TOP = 654846

    helper = None
    awsManager = None


    def __init__(self):
        self.helper = Helper()

        self.awsManager = AwsManager({
            'ACCESS_KEY': self.helper.aws_access_key,
            'SECRET_KEY': self.helper.aws_secret_key,
            'BUCKET_NAME': self.helper.aws_bucket
        })

    def bootstrapClickEventDM(self):
        self.awsManager.download_files_from_s3(
        (self.helper.account_2 + '/ce/01-2017/datamining_1354_2017011105_clickevent.csv.gz',),
        './asset/')

        input('Let\'s check the downloaded file..   ')

        files_decompressed_list = GzipManagerBase.decompress_files(self.awsManager.files_downloaded)

        input('Let\'s check the decompressed file..   ')

        dmAnalyzer = DmAnalyzer()
        filters = [{"label" : "Campaign ID", "values" : [264]}, {"label" : "Insertion ID", "values" : [2616,2614]}]
        output = ["USER ID", "Event type"]
        result_list = dmAnalyzer.filter_dm_and_return_values(files_decompressed_list, filters, output )

        input('Let\'s check the result list..   ')
        for el in result_list:
            print(el)

        input('\nPress enter..   ')

        filtered_result_list = [el for el in result_list if el[1] == 'progress_75' or el[1] == 'progress_100' or el[1] == 'audio_on']
        #filtered_result_list = [el for el in result_list if el[1] != '' and el[0].find('!') != 0]
        input('Let\'s check the filtered list..   ')

        for el in filtered_result_list:
            print(el)

        input('\nPress enter..   ')

        filtered_result_dictionary = dict()
        for el in filtered_result_list:
            if el[0].find('!') != 0:
                if el[0] in filtered_result_dictionary:
                    filtered_result_dictionary[el[0]].add(el[1])
                else:
                    filtered_result_dictionary[el[0]] = {el[1]}

        input('Let\'s check the filtered result dictionary ..   ')
        #print(filtered_result_dictionary)
        for el in filtered_result_dictionary:
            print('Affiche_w Key: {0}, number of event: {1}, events: {2}'.format(el, len(filtered_result_dictionary[el]), filtered_result_dictionary[el]))

        input('\nPress enter..   ')

        list_to_push_to_wam = [[fix_affiche_w(el) + '|' + (str(self.VIDEO_VIEWER_AUDIENCE_PLUS) if len(filtered_result_dictionary[el]) <= 2 else str(self.VIDEO_VIEWER_AUDIENCE_TOP))] for el in filtered_result_dictionary]

        input('Let\'s check the list we are going to push to wam..   ')

        for el in list_to_push_to_wam:
            print(el)

        input('\nPress enter..   ')


        save_list_to_csv('./output', 'top_viewer_list_to_upload.csv', list_to_push_to_wam, [''], ';')


    def bootstrapImpressionDM(self):

        self.awsManager.download_files_from_s3(
        (self.helper.account_1 + '/imp/02-2017/datamining_3788_2017020100_impressionvisibility.csv.gz',),
        './asset/')

        input('Let\'s check the downloaded file..   ')

        files_decompressed_list = GzipManagerBase.decompress_files(self.awsManager.files_downloaded)

        input('Let\'s check the decompressed file..   ')

        dmAnalyzer = DmAnalyzer()
        filters = [{"label" : "Campaign ID", "values" : [4]}]
        output = ["USER ID", "Custom value"]
        result_list = dmAnalyzer.filter_dm_and_return_values(files_decompressed_list, filters, output )

        input('Let\'s check the result list..   ')
        for el in result_list:
            print(el)

        input('\nPress enter..   ')

        filtered_result_list = [el for el in result_list if el[1] != '' and el[0].find('!') != 0]

        input('Let\'s check the filtered list..   ')

        for el in filtered_result_list:
            print(el)

        input('\nPress enter..   ')

        filtered_and_fixed_affiche_w_result_list = [[fix_affiche_w(el[0]), el[1]] for el in filtered_result_list]

        input('Let\'s check the filtered and fixed affiche_w list..   ')

        for el in filtered_and_fixed_affiche_w_result_list:
            print(el)

        input('\nPress enter..   ')

        list_to_push_to_wam = [[str(el[0]) + '|' + str(self.TOP_SPENDER_CUSTOM_SEGMENT_ID)] for el in filtered_and_fixed_affiche_w_result_list if random.randint(0,10) >= 7]

        input('Let\'s check the list we are going to push to wam..   ')

        for el in list_to_push_to_wam:
            print(el)

        input('\nPress enter..   ')

        save_list_to_csv('./output', 'top_spender_list_to_upload.csv', list_to_push_to_wam, [''], ';')

if __name__ == '__main__':
    main = Main()

    direction = input('\nWhich pill you take?')
    if direction == 'red':
        main.bootstrapImpressionDM()
    else:
        main.bootstrapClickEventDM()
