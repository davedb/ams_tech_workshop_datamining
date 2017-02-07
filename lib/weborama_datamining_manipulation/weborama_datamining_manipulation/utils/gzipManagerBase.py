# coding=utf-8
import gzip
import shutil

class GzipManagerBase:
	"""Base class for file compression/decompression. Works on cwd."""

	def __init__(self):
		self.name = 'GzipManagerBase'

	@staticmethod
	def compress_files(files_to_compress):
		"""Compresses a list of files with gzip

		:param files_to_compress: a list of path + filenames to compress
		"""
		for f in files_to_compress:
			with open(f, 'rb') as f_in, gzip.open(f+'.gz', 'wb') as f_out:
			    shutil.copyfileobj(f_in, f_out)

	@staticmethod
	def decompress_files(files_to_decompress):
		"""Decompress files in the same directory where they are

			:param files_to_decompress: a list of path + filenames
			:rtype: list of path + filenames decompressed
		"""
		files_decompressed = []
		for f in files_to_decompress:
			try:
				#print('Il file {0} sta per essere decompresso\n'.format(f))
				input_file = gzip.open(f, 'rb')
				newf = None
				try:
					newf = open(f.rsplit('.',1)[0], 'wb+')
					newf.write(input_file.read())
				finally:
					input_file.close()
					files_decompressed.append(newf.name)
					newf.close()
			except FileNotFoundError:
				files_not_found.append(f)

		return files_decompressed
