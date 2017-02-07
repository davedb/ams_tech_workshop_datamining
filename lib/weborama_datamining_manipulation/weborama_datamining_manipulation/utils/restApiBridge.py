import requests
import base64
import json

class RestApiBridge:
    """Class to read/write from/to weboramaitalia api"""

    def __init__(self):
        self.name = 'RestApiBridge'

    @staticmethod
    def _readGrantFromFile(grant_file):
        f = open(grant_file, 'r')
        ulist = f.read().split(',')
        _u = ulist[0].strip()
        _p = ulist[1].strip()
        f.close()
        return {'username':_u, 'password':_p}

    @staticmethod
    def readFromAPI(url, headers, mongo_grant_file = None, logger = None):
        """Static method that reads from api.

        :param url: string representing api url
        :param headers: string representing headers to send in the requests
        :param mongo_grant_file: string representing filename and path of the grant file
        :param logger: A :class:`Logger <Logger>`

        :rtype: dictionary ``dict {status_code: int, response: dict}``

        Usage::

            >>> from it.weboramaitalia.utils.restApiBridge import RestApiBridge
            >>> RestApiBridge.readFromAPI(...)
        """

        # se il file delle grant è presente recupera username e password
        # lo sostituisce nell'header ricevuto nel campo Authorization
        if mongo_grant_file:
            grant = RestApiBridge._readGrantFromFile(mongo_grant_file)
            user_pwd_string = grant['username'] + ':' + grant['password']

            grant_base_64 = base64.b64encode(user_pwd_string.encode()).decode()
            headers['Authorization'] = headers['Authorization'].replace('{grant_base_64}',grant_base_64)
        else:
            #elimina dall'header il valore authorization
            del headers['Authorization']
            if logger:
                logger.warning('Grant per accedere alle API non ricevute')
            else:
                print('Grant per accedere alle API non ricevute')

        r = requests.get(url, headers=headers)

        return {'status_code' : r.status_code, 'response': r.json()}

    @staticmethod
    def writeToAPI(url, headers, data , mongo_grant_file = None, logger = None):
        """Static method that reads from api

        :param url: string representing api url
        :param headers: string representing headers to send in the requests
        :param mongo_grant_file: string representing filename and path of the grant file
        :param logger: A :class:`Logger <Logger>`
        :rtype: dict {status_code : int, response: dict}

        Usage::

            >>> from it.weboramaitalia.utils.restApiBridge import RestApiBridge
            >>> RestApiBridge.writeToAPI(...)
        """
        # se il file delle grant è presente recupera username e password
        # lo sostituisce nell'header ricevuto nel campo Authorization
        if mongo_grant_file:
            grant = RestApiBridge._readGrantFromFile(mongo_grant_file)
            user_pwd_string = grant['username'] + ':' + grant['password']

            grant_base_64 = base64.b64encode(user_pwd_string.encode()).decode()
            headers['Authorization'] = headers['Authorization'].replace('{grant_base_64}',grant_base_64)
        else:
            #elimina dall'header il valore authorization
            del headers['Authorization']
            if logger:
                logger.warning('Grant per accedere alle API non ricevute')
            else:
                print('Grant per accedere alle API non ricevute')

        r = requests.post(url, headers=headers, data=json.dumps(data))
		#print(r.text, r.status_code)
        try:
            if logger:
                logger.info('Risultato: Status Code: {0},\nStatus Query: {1},\ncreata il: {2},\npermalink: {3}\nFINE SALVATAGGIO\n '.format(r.status_code, r.json()['_status'], r.json()['_created'], r.json()['_links']['self']['href']) )
            else:
                print('Risultato: Status Code: {0},\nStatus Query: {1},\ncreata il: {2},\npermalink: {3}\nFINE SALVATAGGIO\n '.format(r.status_code, r.json()['_status'], r.json()['_created'], r.json()['_links']['self']['href']) )
        except KeyError as e:
            if logger:
                logger.warning('Errore dalle API:\nStatus Code: {0}, risultato: {1}'.format(r.status_code, r.json()))
            else:
                print('Errore dalle API:\nStatus Code: {0}, risultato: {1}'.format(r.status_code, r.json()))

        return {'status_code':r.status_code,'response': r.json()}
