import pysftp
import os

class SftpManager:
    """Base Class for SFTP connections

        :param config: dict ``{'HOST': str, 'USER': str, 'SSHKEY': str}``
    """

    def __init__(self, config):
        self.config = config

    def list_dir(self, directory=None):
        """Lists directory content;

            :param directory: str if you need to list a specific dir, pass the value.
            :rtype: a list containing the files in the dir
        """
        current_list_dir = []
        with pysftp.Connection(self.config['HOST'], username=self.config['USER'], private_key=self.config['SSHKEY']) as sftp:
            if directory:
                try:
                    sftp.chdir(directory)
                except FileNotFoundError as e:
                    sftp.close()
                    return 'directory not found'

            current_list_dir = sftp.listdir()
            sftp.close()
        return current_list_dir


    def download_file(self, path_where_file_is_saved_locally, filename, directory='./'):
        """Downloads a single file.

            :param path_where_file_is_saved_locally: str path to save file downloaded
            :param filename: str the name of the file you need to download
            :param directory: str if you need to downlaod a file from a specific dir, pass the value.

            :rtype: a str containing the path of the file where it is downloaded or *"file not found"* or *"folder not found"*
        """
        response = 'file not found'
        # checks if the path_where_file_is_saved_locally exists
        if os.path.exists(path_where_file_is_saved_locally) == False:
            os.makedirs(path_where_file_is_saved_locally)

        with pysftp.Connection(self.config['HOST'], username=self.config['USER'], private_key=self.config['SSHKEY']) as sftp:
            if directory != './':
                try:
                    sftp.chdir(directory)
                except FileNotFoundError as e:
                    response = 'folder not found'

            try:
                sftp.get(filename, os.path.join(path_where_file_is_saved_locally, filename))
                response = os.path.join(path_where_file_is_saved_locally, filename)
            except FileNotFoundError as e:
                pass

            sftp.close()

        return response
