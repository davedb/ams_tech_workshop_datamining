import os
import sys
import pytest
import datetime as datetime
# aggiunge ai path in cui python ricerca i moduli quello che sta al livello superiore della cartella /test_sample
# tramite questo insert, il path da cui recuperare le librerie è assoluto, quindi pytest può essere chiamato
# da qualsiasi posizione: pytest ./test/test_utils.py o pytest test_utils.py
sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../weborama_datamining_manipulation/'))
from utils import sftpManager


class TestSftpManager:
    WAM_SFTP_HOSTNAME = 'wam-datamining.weborama.com'
    WAM_SFTP_USERNAME = 'wam_3835'
    WAM_SFTP_IDENTITY_FILE = '~/.ssh/smartclip_wam_dm'
    PATH_WHERE_FILE_IS_SAVED_LOCALLY = '../tmp'


    def sftp_manager_test_list_dir(self, directory=None):
        configuration = {
            'HOST' 				                      : self.WAM_SFTP_HOSTNAME,
            'USER'				                      : self.WAM_SFTP_USERNAME,
            'SSHKEY'	                              : self.WAM_SFTP_IDENTITY_FILE,
        }
        realSftpManager = sftpManager.SftpManager(configuration)
        return realSftpManager.list_dir() if directory==None else realSftpManager.list_dir(directory)

    def sftp_manager_test_download_file(self,directory, filename):
        configuration = {
            'HOST' 				                      : self.WAM_SFTP_HOSTNAME,
            'USER'				                      : self.WAM_SFTP_USERNAME,
            'SSHKEY'	                              : self.WAM_SFTP_IDENTITY_FILE,
        }
        realSftpManager = sftpManager.SftpManager(configuration)
        return realSftpManager.download_file(self.PATH_WHERE_FILE_IS_SAVED_LOCALLY, filename, directory)

    # ------
    # Tests:
    # ------

    # ------
    # sftpManager - list_dir:
    # ------

    def test_sftp_list_dir(self):
        # check if list dire returns a list
        assert type(self.sftp_manager_test_list_dir()) is list
        # check if list dir returns directory not found if directory USER wants
        # to list does not exist
        assert self.sftp_manager_test_list_dir(directory='./coldstorag') == 'directory not found'
        # check if list dir on an existing dir returns list
        assert type(self.sftp_manager_test_list_dir(directory='./coldstorage')) is list
        # check if list dir returns a list which length is > 0
        assert len(self.sftp_manager_test_list_dir()) > 0

    # ------
    # sftpManager - download_file:
    # ------

    def test_sftp_download_file(self):
        # try to download a non existant file
        assert self.sftp_manager_test_download_file('./', 'test_file.csv') == 'file not found'

        # check if wrong directory gives folder not found result
        assert self.sftp_manager_test_download_file('./ambaraba', 'test_file.csv') == 'folder not found'

        # check if file is correctly downloaded under the ./ folder
        # get yesterday daytime
        d = datetime.date.today() - datetime.timedelta(1)
        yesterday = d.strftime('%Y%m%d')
        filename = 'datamining_{yesterday}-000000_24.json.zip'.replace('{yesterday}', yesterday)
        assert self.sftp_manager_test_download_file('./', filename) == os.path.join(self.PATH_WHERE_FILE_IS_SAVED_LOCALLY, filename)

        # check if file is correctly downloaded under the ./coldstorage folder
        # get today - 9 daytime
        d = datetime.date.today() - datetime.timedelta(9)
        nine_days_ago = d.strftime('%Y%m%d')
        filename = 'datamining_{nine_days_ago}-000000_24.json.zip'.replace('{nine_days_ago}', nine_days_ago)
        assert self.sftp_manager_test_download_file('./coldstorage', filename) == os.path.join(self.PATH_WHERE_FILE_IS_SAVED_LOCALLY, filename)
