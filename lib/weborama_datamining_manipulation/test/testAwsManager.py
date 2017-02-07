import os
import sys
import pytest
import datetime as datetime
# aggiunge ai path in cui python ricerca i moduli quello che sta al livello superiore della cartella /test_sample
# tramite questo insert, il path da cui recuperare le librerie è assoluto, quindi pytest può essere chiamato
# da qualsiasi posizione: pytest ./test/test_utils.py o pytest test_utils.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../weborama_datamining_manipulation/'))
from utils import awsManager
from utils import genericUtils
from datamining import dmDownloader


class TestAwsManager:
    # set up config dictionary
    config_aws = {
        'AWS_CREDENTIALS_ID'                    : 'data_mining_user',
        'BUCKET_NAME'							: 'weborama-dm-gz',
        'CLIENT_ID'                             :'3834',
        'CLIENT_NAME'                           :'smartclip-IT',
        'DM_TYPE'                               :'imp',
        # per scaricare
        'PATH_TO_GET_DM_ON_S3'					: '3834-smartclip-IT/imp',
        'PATH_WHERE_DM_ARE_SAVED_LOCALLY'		: '../tmp/',
        # per salvare
    }

    def get_files_tuple(self):
        # get today - 31 days
        yesterday_datetime = datetime.datetime.combine(datetime.date.today() \
        - datetime.timedelta(2), datetime.time())
        # today +365
        one_year_from_today = datetime.date.today() + datetime.timedelta(365)
        # giorni da analizzare
        days_splitted_in_24_hours_to_analyze = tuple(
         yesterday_datetime + datetime.timedelta(days=d, hours=h)
         for d in range((yesterday_datetime - yesterday_datetime).days + 1)
         for h in range(0, 24)
        )

        filename_fixed_part = self.config_aws['CLIENT_ID'] + '-' \
        + self.config_aws['CLIENT_NAME'] + '/' + self.config_aws['DM_TYPE'] \
        + '/' + '{MM-YYYY}' + '/datamining_' \
        + self.config_aws['CLIENT_ID'] + '_' + '{YYYYMMDDHH}' \
        + '_' + dmDownloader.DmDownloader.dm_types()[self.config_aws['DM_TYPE']] \
        + '.csv.gz'

        return tuple(
         filename_fixed_part
         .replace('{MM-YYYY}', day_hour.strftime("%m-%Y"))
         .replace('{YYYYMMDDHH}', day_hour.strftime("%Y%m%d%H"))
         for day_hour in days_splitted_in_24_hours_to_analyze
        )



    # ------
    # Tests:
    # ------

    def test_aws_manager_check_elements_are_on_s3(self):
        tuple_of_files_to_be_downloaded = self.get_files_tuple()

        del self.config_aws['AWS_CREDENTIALS_ID']
        with pytest.raises(Exception):
         realAwsManager = awsManager.AwsManager(self.config_aws)

        # add AWS_CREDENTIALS_ID key to config
        self.config_aws['AWS_CREDENTIALS_ID'] = 'data_mining_user'

        realAwsManager = awsManager.AwsManager(self.config_aws)

        prefix_to_check_on_s3 = self.config_aws['CLIENT_ID'] + '-' \
        + self.config_aws['CLIENT_NAME'] \
        + '/' + self.config_aws['DM_TYPE'] + '/'

        tuple_of_elements_found = realAwsManager.check_elements_are_on_s3(tuple_of_files_to_be_downloaded, prefix_to_check_on_s3)
        # print(tuple_of_elements_found)
        assert type(tuple_of_elements_found) is tuple
        assert len(tuple_of_elements_found) == 24
        assert tuple_of_elements_found == tuple_of_files_to_be_downloaded

    def test_aws_manager_download_files_from_s3(self):
        tuple_of_files_to_be_downloaded = self.get_files_tuple()

        del self.config_aws['AWS_CREDENTIALS_ID']
        with pytest.raises(Exception):
         realAwsManager = awsManager.AwsManager(self.config_aws)
        # add AWS_CREDENTIALS_ID key to config
        self.config_aws['AWS_CREDENTIALS_ID'] = 'data_mining_user'
        realAwsManager = awsManager.AwsManager(self.config_aws)

        prefix_to_check_on_s3 = self.config_aws['CLIENT_ID'] + '-' \
        + self.config_aws['CLIENT_NAME'] \
        + '/' + self.config_aws['DM_TYPE'] + '/'

        path_to_save_dm_locally = '../tmp'

        files_downloaded = realAwsManager.download_files_from_s3(
         tuple_of_files_to_be_downloaded,
         path_to_save_dm_locally
        )

        assert type(files_downloaded) is list
        assert len(files_downloaded) == 24


    # ------
    # awsManager - upload files :
    # ------

    def test_aws_manager_upload_file_on_S3(self):
        # crea il file:
        genericUtils.save_list_to_csv('../output', 'test.csv', [[1,2,3,4],[1,2,3,4],[1,2,3,4]], ['a','b','c','d'], '\t')

        config = {
            'AWS_CREDENTIALS_ID'                            : 'hannah',
            'BUCKET_NAME'							        : 'weboramait.hannah',
            # per scaricare
            'PATH_WHERE_OUTPUT_IS_SAVED_LOCALLY'		    : '../output',
            'OUTPUT_FILENAME'		                        : 'test.csv',
            'MAIN_FOLDER_FOR_OUTPUT_ON_S3'		            : 'output-temp',
        }
        myAwsManager = awsManager.AwsManager(config)

        assert myAwsManager.save_file_on_S3()['ResponseMetadata']['HTTPStatusCode'] == 200
