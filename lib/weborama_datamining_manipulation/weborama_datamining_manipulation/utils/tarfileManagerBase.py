# coding=utf-8
import tarfile
import os

class TarfileManagerBase:
    """Base Class for decompression of tar files."""
    def __init__(self):
        self.name = 'TarfileManagerBase'

    @staticmethod
    def decompress_files(files_to_decompress):
        """Extract all members from the archive to its directory path

        :param files_to_decompress: list of archives"""
        # TODO: verify the extraction of files happen where the archive is or in
        # the cwd
        for f in files_to_decompress:
			#print('Il file {0} sta per essere decompresso\n'.format(f))
            tar = tarfile.open(f)
            tar.extractall()
            tar.close()
            #os.remove(f)
