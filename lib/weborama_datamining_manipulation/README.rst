pip3 install -r requirements.txt

build the doc:
>>>cd doc/
>>>PYTHONPATH=~/weborama_datamining_manipulation/ make html

pytest lib/test/testSftpManager.py::TestSftpManager::test_sftp_download_file
