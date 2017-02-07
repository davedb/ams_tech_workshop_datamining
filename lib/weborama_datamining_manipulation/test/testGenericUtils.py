import os
import sys
import pytest
import datetime as datetime
# aggiunge ai path in cui python ricerca i moduli quello che sta al livello superiore della cartella /test_sample
# tramite questo insert, il path da cui recuperare le librerie è assoluto, quindi pytest può essere chiamato
# da qualsiasi posizione: pytest ./test/test_utils.py o pytest test_utils.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../weborama_datamining_manipulation/'))
from utils import genericUtils
from utils import logger
from utils.gzipManagerBase import GzipManagerBase
from utils.tarfileManagerBase import TarfileManagerBase
from utils.zipFileManagerBase import ZipFileManagerBase


class TestGenericUtils:


    # ------
    # Tests:
    # ------

    # ------
    # genericUtils - time functions:
    # ------

    def test_generic_utils_time_functions(self):
        # get today - 31 days
        d = datetime.date.today() - datetime.timedelta(31)
        one_month_ago_1 = d.strftime('%d/%m/%Y')
        one_month_ago_2 = d.strftime('%Y-%m-%d')
        one_month_ago_3 = d.strftime('%d-%m-%Y')
        assert type(genericUtils.str2unixtime(one_month_ago_1)) is int
        assert type(genericUtils.str2unixtime(one_month_ago_2)) is int
        assert genericUtils.str2unixtime(one_month_ago_1) == genericUtils.str2unixtime(one_month_ago_2)

        assert type(genericUtils.unixtime2datetime(genericUtils.str2unixtime(one_month_ago_2))) is datetime.datetime

        # verify string_2_datetime accepts only date separated by "-"
        with pytest.raises(ValueError):
            assert type(genericUtils.string_2_datetime(one_month_ago_1)) is datetime

        assert type(genericUtils.string_2_datetime(one_month_ago_2)) is datetime.datetime
        assert type(genericUtils.string_2_datetime(one_month_ago_3)) is datetime.datetime
        assert genericUtils.string_2_datetime(one_month_ago_3) == genericUtils.string_2_datetime(one_month_ago_2)

    # ------
    # genericUtils - set cookie :
    # ------

    def test_set_cookie_and_launch_url(self):
        assert type(genericUtils.set_cookie_and_launch_url('abcredtespoa', 'http://example.com/')) == dict
        assert genericUtils.set_cookie_and_launch_url('abcredtespoa', 'http://example.com/')['status_code'] == 200
        assert genericUtils.set_cookie_and_launch_url('abpoa', 'http://example.c/')['ok'] == 0
        assert genericUtils.set_cookie_and_launch_url('abpoa', 'http://example.c/')['status_code'] == 0

    # ------
    # genericUtils - save csv :
    # ------

    def test_save_to_csv(self):
        genericUtils.save_list_to_csv('../output', 'test.csv', [[1,2,3,4],[1,2,3,4],[1,2,3,4]], ['a','b','c','d'], '\t')



    # ------
    # logger -  :
    # ------

    def test_logger(self):
        logger_config ={
            'IS_DEV' 				               	: True,
            'IS_PREPROD'							: False,
            'IS_PROD'		 		      			: False,
            'LOGGING_FILE'				          	: '../log/test_log.log',
        }

        realLoggerFactory = logger.Logger(logger_config)
        myLogger = realLoggerFactory.create_logger('Pippo')

        myLogger.info('Prova')

        logger_config= {
            'IS_DEV': True,
            'IS_PREPROD': False,
            'IS_PROD': False,
            'LOGGING_FILE': '../log/test_log.log'
        }


        realLoggerFactory2 = logger.Logger(logger_config)

        myLogger2 = realLoggerFactory2.create_logger('Pluto')

        myLogger2.info('Prova 2')


    # ------
    # GzipManager -  :
    # ------

    def test_gzipManager(self):
        genericUtils.save_list_to_csv('../output', 'test.csv', [[1,2,3,4],[1,2,3,4],[1,2,3,4]], ['a','b','c','d'], '\t')
        GzipManagerBase.compress_files(['../output/test.csv'])
        GzipManagerBase.decompress_files(['../output/test.csv.gz'])

    # ------
    # TarfileManagerBase -  :
    # ------

    def test_tarfileManager(self):
        # decompress the files in the cwd
        try:
            TarfileManagerBase.decompress_files(['../tmp/archive.tar.gz'])
        except FileNotFoundError as e:
            pass



    # ------
    # ZipFileManagerBase -  :
    # ------

    def test_zipManager(self):
        try:
            ZipFileManagerBase.extract_each_file_from_archive(['../tmp/datamining_20161218-000000_24.json.zip'])
        except FileNotFoundError as e:
            pass
