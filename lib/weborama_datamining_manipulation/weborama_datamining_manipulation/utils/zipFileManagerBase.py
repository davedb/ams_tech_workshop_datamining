# coding=utf-8
import os.path
import zipfile


class ZipFileManagerBase:
    """Base class for file decompression. Works on cwd."""

    def __init__(self):
        self.name = 'ZipFileManagerBase'

    @staticmethod
    def extract_each_file_from_archive(archives_to_decompress):
        """Extract all the files inside a .zip archive in the same folder.

            :param archives_to_decompress: list of archives to decompress
            :rtype: a list of files unzipped"""
        files_extracted = []
        for archive in archives_to_decompress:
            same_path_as_archive = os.path.split(archive)[0]
            with zipfile.ZipFile(archive) as myzip:
                for el in myzip.namelist():
                    try:
                        files_extracted.append(myzip.extract(myzip.getinfo(el),
                        same_path_as_archive))
                    except Exception as e:
                        continue

        return files_extracted
