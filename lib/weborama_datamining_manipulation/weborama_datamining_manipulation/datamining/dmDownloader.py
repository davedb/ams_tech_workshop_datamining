from os import listdir
import time
import datetime as datetime
import re
from utils.awsManager import AwsManager
from utils.sftpManager import SftpManager
from utils.gzipManagerBase import GzipManagerBase
from utils.zipFileManagerBase import ZipFileManagerBase

class DmDownloader:
    """Class to download WCM Datamining. It manages GMT by default.

    **beware**: you need to add the path of weborama_datamining_manipulation, e.g.::

	sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../shared/lib/weborama_datamining_manipulation'))

    Usage::

        >>> import DmDownloader
        >>> dmDownloader = DmDownloader()
        >>> dmDownloader.config_aws = {...}
        >>> dmDownloader.logger = logger
        >>> dmDownloader.download_WCM_datamining_from_s3(...)
        >>> dmDownloader.files_downloaded()

    Static Methods:
        - gmt_difference()
        - dm_types()
    Public Properties:
        - config_aws
        - logger
        - fix_gmt
        - files_downloaded
        - files_downloaded_and_decompressed
    Public Methods:
        - download_WCM_datamining_from_s3
        - download_WAM_datamining_from_sftp
    """

    def __init__(self):
        self.name   = 'DmDownloader'
        self._config_aws = None
        self._config_sftp = None
        self._files_downloaded = []
        self._files_downloaded_and_decompressed = []
        self._logger = None
        self._fix_gmt = True

    @staticmethod
    def gmt_difference():
    	"""Returns current gmt difference based on Rome GMT"""
    	return 1 if time.localtime().tm_isdst == 0 else 2

    @staticmethod
    def dm_types():
        """Returns a dictionarty containing datamining types.

        Keys are

        - imp,
        - ce,
        - conv;

        Values are

        - impressionvisibility,
        - clickevent,
        - conversion"""

        return {
            'imp'   : 'impressionvisibility',
            'ce'    : 'clickevent',
            'conv'  : 'conversion'
        }


    @property
    def config_aws(self):
        """Returns AWS configuration; CLIENT_ID and CLIENT_NAME are optional

        :rtype: dict ``{ 'AWS_CREDENTIALS_ID': str, 'BUCKET_NAME': str, 'CLIENT_ID': str,'CLIENT_NAME': str}``"""
        return self._config_aws

    @config_aws.setter
    def config_aws(self, value):
        """Set AWS configuration
        :param value: dict
            {
                'AWS_CREDENTIALS_ID'    : str,
                'BUCKET_NAME'           : str,
                #optional
                'CLIENT_ID'             : str,
                'CLIENT_NAME'           : str
            }
        """
        try:
        	value['AWS_CREDENTIALS_ID']
        	value['BUCKET_NAME']
        except KeyError as e:
        	raise Exception('Error: you need to specify' \
            '**AWS_CREDENTIALS_ID** and **BUCKET_NAME**')

        self._config_aws = value

    @property
    def config_sftp(self):
        """Returns a dictionary containing sftp settings

        :rtype: dict ``{'HOST': str, 'USER': str, 'SSHKEY': str}``
        """
        return self._config_sftp

    @config_sftp.setter
    def config_sftp(self, value):
        """Set SFTP configuration
        :param value: dict
            {
                'HOST'    : str,
                'USER'    : str,
                'SSHKEY'  : str
            }
        """
        try:
        	value['HOST']
        	value['USER']
        	value['SSHKEY']
        except KeyError as e:
        	raise Exception('Error: you need to specify' \
            '**HOST**, **USER** and **SSHKEY**')

        self._config_sftp = value

    @property
    def logger(self):
        """Returns a :class:`Logger <Logger>`"""
        return self._logger

    @logger.setter
    def logger(self, value):
        """Set logger
        :param value: <Logger>
        """
        self._logger = value

    @property
    def fix_gmt(self):
        """Returns if the time must be fixed considering Rome GMT

        :rtype: bool"""
        return self._fix_gmt

    @fix_gmt.setter
    def fix_gmt(self, value):
        """Set if the time must be fixed considering Rome GMT

        :param value bool"""
        self._fix_gmt = value


    @property
    def files_downloaded(self):
        """Returns a list of files saved as they are (ie: compressed),
        if no files are saved len(list) == 0

        :rtype: a list of path + filename
        """
        return self._files_downloaded

    @property
    def files_downloaded_and_decompressed(self):
        """Returns a list of files saved and decompressed,
        if no files are saved len(list) == 0

        :rtype: a list of path + filename"""
        return self._files_downloaded_and_decompressed

    def download_WAM_datamining_from_sftp(self,
    start_date,end_date,\
    path_to_save_dm_locally):
        """Downloads adn decompresses WAM datamining files from SFTP.

        :param start_date: datetime.datetime of the day you want to start download dm
        :param end_date: datetime.datetime of the day you want to end the dm downlaod
        :param path_to_save_dm_locally: str defining the path where to save dm
        """
        #clean the property
        self._files_downloaded = []
        self._files_downloaded_and_decompressed = []

        if self.config_sftp == None:
            raise Exception("Attention: you need to configure sftp connection\
            settings via config_sftp")

        filename_fixed_part = 'datamining_{date_to_download}-000000_24.json.zip'
        files_to_be_downloaded = []

        if start_date == end_date:
            files_to_be_downloaded.append(filename_fixed_part
             .replace(
              '{date_to_download}',
              start_date.strftime('%Y%m%d')
             )
            )
        else:
            files_to_be_downloaded =  [filename_fixed_part
             .replace(
              '{date_to_download}',
              day.strftime('%Y%m%d')
             ) for day in [
              start_date + datetime.timedelta(days = d)
              for d in range((end_date - start_date).days + 1)
             ]
            ]


        sftpManager = SftpManager(self.config_sftp)
        file_list_on_sftp = sftpManager.list_dir() #./home
        if file_list_on_sftp == 'directory not found':
            if self.logger:
                self.logger.error('{0} - Error: the directory: {1} to look for files on sftp does not exist.'.format(self.name,directory))
                return
        #####
        elements_to_get = [ el for check_single_file in files_to_be_downloaded for el in file_list_on_sftp if re.search(check_single_file, el)]

        if len(elements_to_get) >= 1:
            for dm in elements_to_get:
                response = sftpManager.download_file(path_to_save_dm_locally, dm)

                if response != 'file not found' and 'folder not found':
                    self._files_downloaded.append(response)

                    files_extracted = ZipFileManagerBase.extract_each_file_from_archive([response])
                    for el in files_extracted:
                        self._files_downloaded_and_decompressed.append(el)

        elif len(elements_to_get) == 0:
            file_list_on_sftp = sftpManager.list_dir('coldstorage')
            if file_list_on_sftp == 'directory not found':
                if self.logger:
                    self.logger.error('{0} - Error: the directory: {1} to look for files on sftp does not exist.'.format(self.name,directory))
                    return

            elements_to_get = [ el for check_single_date in check_dates for el in file_list_on_sftp if re.search(check_single_date, el)]

            response = sftpManager.download_file(path_to_save_dm_locally, dm)
            if response != 'file not found' and 'folder not found':
                self._files_downloaded.append(response)
                files_extracted = ZipFileManagerBase.extract_each_file_from_archive([response])
                for el in files_extracted:
                    self._files_downloaded_and_decompressed.append(el)


    def download_WCM_datamining_from_s3(
    self,
    dm_type,
    start_date,
    end_date,
    path_to_save_dm_locally
    ):
        """Downloads and decompresses datamining files from AWS S3 bucket,
            It checks via awsManager if files are on s3, filenames are formed
            like this:\n
            **| bucket | client id-client name | dm type | month-year | datamining_client id_YYYYMMDDHH_dmtype.csv.gz**\n
            ``weborama-dm-gz/1007-coop/conv/01-2016/datamining_1007_2016010100_conversion.csv.gz``\n
            *bucket, client id and client name are found in config_aws prop*

            :param dm_type: string representing the dm type (imp,ce,conv)
            :param start_date: datetime.datetime the starting date for
                                downloading dm
            :param end_date: datetime.datetime the ending date (greater
                                than starting date) for downloading dm
            :param path_to_save_dm_locally: string representing the path
                                            where dm are saved locally
        """
        #clean the property
        self._files_downloaded = []
        self._files_downloaded_and_decompressed = []

        tuple_of_files_to_be_downloaded = self.__evaluates_single_date_hours_dm_files_to_download(
         dm_type,
         start_date,
         end_date
        )

        self.awsManager = AwsManager(self.config_aws)

        prefix_to_check_on_s3 = self.config_aws['CLIENT_ID'] + '-' \
        + self.config_aws['CLIENT_NAME'] \
        + '/' + dm_type + '/'

        tuple_of_files_on_s3 = self.awsManager.check_elements_are_on_s3(
         tuple_of_files_to_be_downloaded,
         prefix_to_check_on_s3
        )

        if len(tuple_of_files_to_be_downloaded) == len(tuple_of_files_on_s3):
            self.awsManager.download_files_from_s3(
             tuple_of_files_to_be_downloaded,
             path_to_save_dm_locally
            )

            self._files_downloaded = self.awsManager.files_downloaded
            # decompress files
            self._files_downloaded_and_decompressed = \
            GzipManagerBase.decompress_files(self._files_downloaded)

        return self

    def __evaluates_single_date_hours_dm_files_to_download(
     self,
     dm_type,
     start_date,
     end_date
    ):

        """Evaluates the names of the files to be downloaded based on
            dm_type, start_date end_date, and it manages the gmt differnce
            it creates the paths and names of the files to download.
            they are formed like this:
            | bucket | client id-client name | dm type | month-year | datamining_client id_YYYYMMDDHH_dmtype.csv.gz
            weborama-dm-gz/1007-coop/conv/01-2016/datamining_1007_2016010100_conversion.csv.gz
            bucket, client id and client name are found in config_aws prop

            :param dm_type: string representing the dm type (imp,ce,conv)
            :param start_date: datetime.datetime the starting date for
                                downloading dm
            :param end_date: datetime.datetime the ending date (greater
                                than starting date) for downloading dm
            :rtype: A tuple of file names to be downloaded
        """
        # giorni da analizzare

        days_splitted_in_24_hours_to_analyze = tuple(
         start_date + datetime.timedelta(days = d, hours = h - \
         (DmDownloader.gmt_difference() if self.fix_gmt else 0))
         for d in range((end_date - start_date).days + 1)
         for h in range(0, 24)
        )

        filename_fixed_part = self.config_aws['CLIENT_ID'] + '-' \
        + self.config_aws['CLIENT_NAME'] + '/' + dm_type \
        + '/' + '{MM-YYYY}' + '/datamining_' \
        + self.config_aws['CLIENT_ID'] + '_' + '{YYYYMMDDHH}' \
        + '_' + DmDownloader.dm_types()[dm_type] \
        + '.csv.gz'

        tuple_of_files_to_be_downloaded = tuple(
         filename_fixed_part
         .replace('{MM-YYYY}', day_hour.strftime("%m-%Y"))
         .replace('{YYYYMMDDHH}', day_hour.strftime("%Y%m%d%H"))
         for day_hour in days_splitted_in_24_hours_to_analyze
        )
        return tuple_of_files_to_be_downloaded
