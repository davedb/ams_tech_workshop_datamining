import os
import sys
import datetime
import pytest
# aggiunge ai path in cui python ricerca i moduli quello che sta al livello superiore della cartella /test_sample
# tramite questo insert, il path da cui recuperare le librerie è assoluto, quindi pytest può essere chiamato
# da qualsiasi posizione: pytest ./test/test_utils.py o pytest test_utils.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../weborama_datamining_manipulation/'))

from datamining import dmDownloader
from datamining import dmAnalyzer
from datamining import dmWAMAnalyzer
from utils import logger
class TestDataminingUtils:
    configWCMDm = {
        'AWS_CREDENTIALS_ID'                    : 'data_mining_user',
        'BUCKET_NAME'							: 'weborama-dm-gz',
        # per scaricare
        'CLIENT_ID' 					        : '3834',
        'CLIENT_NAME' 					        : 'smartclip-IT',
        'DM_TYPE' 					            : 'imp',
        'PATH_WHERE_DM_ARE_SAVED_LOCALLY'		: '../tmp/',
        # per salvare
    }

    configWAMDm = {
        'PATH_WHERE_DM_ARE_SAVED_LOCALLY'		: '../tmp/',
    }

    WAM_SFTP_HOSTNAME = 'wam-datamining.weborama.com'
    WAM_SFTP_USERNAME = 'wam_3835'
    WAM_SFTP_IDENTITY_FILE = '~/.ssh/smartclip_wam_dm'
    PATH_WHERE_FILE_IS_SAVED_LOCALLY = '../tmp'

    configSFTP = {
        'HOST' 				                      : WAM_SFTP_HOSTNAME,
        'USER'				                      : WAM_SFTP_USERNAME,
        'SSHKEY'	                              : WAM_SFTP_IDENTITY_FILE,
    }

    def test_dmDownloader_download(self):
        # before_yesterday_date = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(3), datetime.time())
        # #download dm and decompress for 2 days
        # realDmDownloader.download_and_decompress_datamining(before_yesterday_date,yesterday_date, None )
        # assert len(realDmDownloader.files_saved) == 48
        # assert len(realDmDownloader.files_saved_and_decompressed) == 48
        yesterday_date = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(2), datetime.time())


        realDmDownloader = dmDownloader.DmDownloader()
        realDmDownloader.config_aws = self.configWCMDm

        files_downloaded = realDmDownloader.download_WCM_datamining_from_s3(
         dm_type = self.configWCMDm['DM_TYPE'],
         start_date=yesterday_date,
         end_date=yesterday_date,
         path_to_save_dm_locally=self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY']
        ).files_downloaded

        assert len(files_downloaded) == 24

        assert len(realDmDownloader.files_downloaded_and_decompressed) == 24

        before_yesterday_date = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(3), datetime.time())

        # verifica che il fix gmt funzioni correttamente
        str_to_check = before_yesterday_date.strftime('%Y%m%d23')

        assert len([f for f in files_downloaded if f.find(str_to_check) > 0]) == 1

        files_downloaded = realDmDownloader.download_WCM_datamining_from_s3(
         dm_type='imp',
         start_date=before_yesterday_date,
         end_date=yesterday_date,
         path_to_save_dm_locally='../tmp'
        ).files_downloaded

        assert len(files_downloaded) == 48

        assert len(realDmDownloader.files_downloaded_and_decompressed) == 48

    def test_dmDownloader_gmt_fix_false_download(self):
        yesterday_date = datetime.datetime.combine(datetime.date.today() \
        - datetime.timedelta(2), datetime.time())
        before_yesterday_date = datetime.datetime.combine(datetime.date.today() \
        - datetime.timedelta(3), datetime.time())

        str_to_check = before_yesterday_date.strftime('%Y%m%d23')

        realDmDownloader = dmDownloader.DmDownloader()
        realDmDownloader.config_aws = self.configWCMDm
        realDmDownloader.fix_gmt = False

        files_downloaded = realDmDownloader.download_WCM_datamining_from_s3(
         dm_type = self.configWCMDm['DM_TYPE'],
         start_date=yesterday_date,
         end_date=yesterday_date,
         path_to_save_dm_locally=self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY']
        ).files_downloaded

        assert len(files_downloaded) == 24
        assert len([f for f in files_downloaded if f.find(str_to_check) > 0]) == 0


    def test_dmDownloader_wam_download(self):
        before_yesterday_date = datetime.datetime.combine(datetime.date.today() \
        - datetime.timedelta(3), datetime.time())

        yesterday_date = datetime.datetime.combine(datetime.date.today() \
        - datetime.timedelta(2), datetime.time())

        realDmDownloader = dmDownloader.DmDownloader()
        #

        with pytest.raises(Exception):
            realDmDownloader.download_WAM_datamining_from_sftp(yesterday_date,
            yesterday_date, self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY'])

        realDmDownloader.config_sftp = self.configSFTP

        realDmDownloader.download_WAM_datamining_from_sftp(yesterday_date,
        yesterday_date, self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY'])

        assert len(realDmDownloader.files_downloaded) == 1
        assert len(realDmDownloader.files_downloaded_and_decompressed) == 2

        realDmDownloader.download_WAM_datamining_from_sftp(before_yesterday_date,
        yesterday_date, self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY'])

        assert len(realDmDownloader.files_downloaded) == 2
        assert len(realDmDownloader.files_downloaded_and_decompressed) == 4


    def test_dmAnalyzer(self):
        yesterday_date = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(2), datetime.time())

        realDmDownloader = dmDownloader.DmDownloader()
        realDmDownloader.config_aws = self.configWCMDm
        #download dm and decompress for 1 day
        realDmDownloader.download_WCM_datamining_from_s3( dm_type = self.configWCMDm['DM_TYPE'],
        start_date=yesterday_date,
        end_date=yesterday_date,
        path_to_save_dm_locally=self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY']
        )

        realDmAnalyzer = dmAnalyzer.DmAnalyzer()

        filters = [{"label":"Project name", "values": ["idSync"]}]
        output = ["USER ID", "Custom value"]

        filtered_list = realDmAnalyzer.filter_dm_and_return_values(realDmDownloader.files_downloaded_and_decompressed, filters, output)
        assert type(filtered_list) is list
        assert type(realDmAnalyzer.analyze_files_and_return_affiche_w_set(realDmDownloader.files_downloaded_and_decompressed)) is set

    def test_dmWAMAnalyzer(self):
        yesterday_date = datetime.datetime.combine(datetime.date.today() - datetime.timedelta(2), datetime.time())
        loggerFactory = logger.Logger({'IS_DEV':True, 'LOGGING_FILE': '../output/logfile.log'})
        myLogger = loggerFactory.create_logger('unit_test_logger')
        realDmDownloader = dmDownloader.DmDownloader()

        realDmDownloader.config_sftp = self.configSFTP
        realDmDownloader.logger = myLogger

        realDmDownloader.download_WAM_datamining_from_sftp(yesterday_date,
        yesterday_date, self.configWCMDm['PATH_WHERE_DM_ARE_SAVED_LOCALLY'])

        realDmWAMAnalyzer = dmWAMAnalyzer.DmWAMAnalyzer()
        realDmWAMAnalyzer.logger = myLogger
        list_of_elements = realDmWAMAnalyzer.get_fields_from_dataming_lines(realDmDownloader.files_downloaded_and_decompressed, ['uid', 'sociodemos'])


        # realDmWAMAnalyzer = dmWAMAnalyzer.DmWAMAnalyzer()
        # list_of_elements = realDmWAMAnalyzer.get_fields(['../tmp/datamining_20161231-000000_24.json'], ['uid', 'sociodemos', 'baieccati'])

        assert type(list_of_elements) is list
