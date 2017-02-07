# coding=utf-8
import boto3
import botocore
import re
import os.path


class AwsManager:
	"""Class to leverage AWS services

		:param config: dict ``{'AWS_CREDENTIALS_ID': str,'BUCKET_NAME': str}``
	"""
	def __init__(self, config):
		self.name = 'AwsManager'
		self.config = config
		self._logger = None
		self._files_downloaded = []
		# se esiste la chiave AWS_CREDENTIALS_ID carica il profilo
		try:
			self.session = boto3.Session(profile_name=self.config['AWS_CREDENTIALS_ID'])
			# Any clients created from this session will use credentials
			# from the self.config['AWS_CREDENTIALS_ID'] section of ~/.aws/credentials.
			self.s3 = self.session.resource('s3')
		except KeyError as e:
			try:
				self.session = boto3.Session(
				    aws_access_key_id=self.config['ACCESS_KEY'],
				    aws_secret_access_key=self.config['SECRET_KEY']
				)
				self.s3 = self.session.resource('s3')
			except KeyError as e:
				raise Exception('Error: you need to specify **AWS_CREDENTIALS_ID**')

		self.bucket = self.s3.Bucket(self.config['BUCKET_NAME'])


	@property
	def logger(self):
		"""Returns the logger (if set)"""
		return self._logger

	@logger.setter
	def logger(self, value):
		"""Set logger
		:param value: <Logger>
		"""
		self._logger = value


	@property #Lista in cui vengono salvati i nomi dei file scaricati
	def files_downloaded(self):
		"""
			Returns a list of files saved (full path + filename)
		"""
		return self._files_downloaded

	@files_downloaded.setter
	def files_downloaded(self, value):
		self._files_downloaded = value

	def save_file_on_S3(self):
		"""Saves a CSV file to S3; **restrieves data from the config property**:

		- BUCKET_NAME
		- OUTPUT_FILENAME
		- PATH_WHERE_OUTPUT_IS_SAVED_LOCALLY
		- MAIN_FOLDER_FOR_OUTPUT_ON_S3
		"""
		file_to_save = open(os.path.join(self.config['PATH_WHERE_OUTPUT_IS_SAVED_LOCALLY'], self.config['OUTPUT_FILENAME']), 'rb')

		s3_response = self.s3.Object(self.config['BUCKET_NAME'], self.config['MAIN_FOLDER_FOR_OUTPUT_ON_S3'] + '/' + self.config['OUTPUT_FILENAME']).put(ACL='public-read',Body=file_to_save, ContentType='text/csv')

		return(s3_response)



	def check_elements_are_on_s3(self, files_to_check_on_s3, prefix=None):
		"""Checks if elements on files_to_check_on_s3 are on S3

			:param files_to_check_on_s3: tuple of path+filenames
			:param prefix: string of path where files should be on S3
			:rtype: tuple of elements found on s3
		"""
		if prefix == None:
			prefix=''
		object_on_s3_generator = self.bucket.objects.filter(Prefix=prefix)
		return tuple(obj.key for obj in object_on_s3_generator
		 for f in files_to_check_on_s3 if(re.search(re.compile(f), obj.key))
		)


	def download_files_from_s3(self, files_to_be_downloaded, path_to_save_dm_locally):
		"""Manages downloads from S3

			:param files_to_be_downloaded: a tuple of files to be downloaded; you need to specify the whole path but the bucket.
			:param path_to_save_dm_locally: string of the path to download files to
			:rtype: a list of files saved on local machine
		"""

		#reset values:
		self.files_downloaded = []
		#crea le cartelle del path, se non sono presenti
		try:
			os.makedirs(path_to_save_dm_locally)
		except FileExistsError:
			pass

		if self.logger:
			self.logger.debug('{1} {0} Data Mining files in corso di scaricamento...\n'
			.format(self.total_files_to_download, self.name))
		#crea un generator per ciclare la lista e gestire il completo scaricamento
		files_generator = (f for f in files_to_be_downloaded)
		#cicla il generator fino a quando non termina la lista
		loopIt = True
		while loopIt:
			try:
				f = next(files_generator)
				#elabora il path dove scaricare il file
				save_file_path = os.path.join(
				 path_to_save_dm_locally,
				 f.split('/')[-1]
				)
				#print(save_file_path)
				self.s3.Object(self.config['BUCKET_NAME'], f) \
				.download_file(save_file_path)
				#salva il percorso del file scaricato
				self.files_downloaded.append(save_file_path)
			except StopIteration:
				loopIt = False
				if self.logger:
					self.logger.info('{1} Scaricati {0} Data Mining File \n'.format(len(self.files_downloaded), self.name))

		return self.files_downloaded
