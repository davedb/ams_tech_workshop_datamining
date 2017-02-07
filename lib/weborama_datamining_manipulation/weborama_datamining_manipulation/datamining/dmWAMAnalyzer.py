# coding=utf-8
import json
import re

class DmWAMAnalyzer():
    """Class to analyze wam datamining
    Usage::

        >>> import DmWAMAnalyzer
        >>> dmWAMAnalyzer = DmWAMAnalyzer()
        >>> dmWAMAnalyzer.logger = logger
        >>> filtered_list = dmWAMAnalyzer.get_fields(...)

    Public Properties::
        - logger
    Public Methods::
        - get_fields_from_dataming_lines
    """

    def __init__(self):
        self.name = ':: DmWAMAnalyzer >'
        self._logger = None


    @staticmethod
    def fields_available():
        """Returns a dictionary that contains all the fields available in the datamining"""
        return {
        'uid':'uid',
        'profile' : ['last_update', 'clusters', 'geoloc', 'segments','sociodemos'],
        'wam': ['custom_segments', 'techno', 'audiences']
        }

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

    def get_fields_from_dataming_lines(self, files_to_analyze, fields):
        """Analyzes the files in files_to_analyze list,
         looking for fields described in fields param

         :param files_to_analyze: list of path + name of files to be analyzed
         :param fields: list of fields we want the method to return

         :rtype: list of dictionaries having as key the values of param fields and for values the values found in the datamining files e.g.: fields = ``['uid', 'sociodemos']`` return ``[{'uid': '1x6JsSby2YLX', 'sociodemos': {'4': 7, '13': 10, '15': 2}}]``
         """

        fields_available_list = [el for el in DmWAMAnalyzer.fields_available().values() if type(el) is list]

        for single_field in fields:
            element_in_fields_available = False
            if single_field in DmWAMAnalyzer.fields_available().values():
                element_in_fields_available = True
            else:
                for single_list in fields_available_list:
                    if single_field in single_list:
                        element_in_fields_available = True

            if element_in_fields_available == False:
                if self.logger:
                    self.logger.warning("Field {0} not available in datamining".format(single_field))

        fields_to_return = []
        # regular expression per il controllo che i dati ricevuti siano alfanumerici
        # + "/" e ".", presenti nel uid
        p = re.compile('^[a-zA-Z0-9\/.-]+$')

        for file_to_analyze in files_to_analyze:
            # skip, if present in the list, the file: datamining_20161231-000000_24.json.meta
            if file_to_analyze.find('.meta') > 0:
                continue
            f = open(file_to_analyze, 'r')

            for line in f:
                current_fields_to_return = {}
                json_line = json.loads(line)

                uid = ''
                last_update = ''
                clusters = ''
                geoloc = ''
                segments = ''
                sociodemos = ''
                last_update_wam = ''
                custom_segments = ''
                techno = ''
                audiences = ''

                if 'uid' in fields:
                    uid = json_line['uid']
                    current_fields_to_return['uid'] = uid

                ############ check & cleaning last_update ############
                # TODO: 2 values of last update (last_update, last_update_wam)
                # from datamining are merged in one value
                # need to fix this.
                if 'last_update' in fields:
                    try:
                        last_update = json_line['events']['profile']['last_update']
                        if(re.match(p, str(last_update)) == False): last_update= ''

                        last_update_wam = json_line['events']['wam']['last_update']
                        if(re.match(p, str(last_update_wam)) == False): last_update_wam= ''
                        current_fields_to_return['last_update'] = last_update if last_update != '' else last_update_wam
                    except KeyError as e:
                        pass
                        # self.config.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )


                ############ check & cleaning clusters ############
                if 'clusters' in fields:
                    try:
                        clusters = json_line['events']['profile']['clusters']
                        for k, v in clusters.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in clusters'.format(str(k)))
                                del clusters[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in clusters'.format(str(v)))
                                del clusters[k]

                        current_fields_to_return['clusters'] = clusters
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning geoloc ############
                if 'geoloc' in fields:
                    try:
                        geoloc = json_line['events']['profile']['geoloc']

                        for k,v  in geoloc.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in geoloc'.format(str(k)))
                                del geoloc[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in geoloc'.format(str(v)))
                                del geoloc[k]
                        current_fields_to_return['geoloc'] = geoloc
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning segments ############
                if 'segments' in fields:
                    try:
                        segments = json_line['events']['profile']['segments']

                        for k,v in segments.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in segments'.format(str(k)))
                                del segments[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in segments'.format(str(v)))
                                del segments[k]
                        current_fields_to_return['segments'] = segments
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning sociodemos ############
                if 'sociodemos' in fields:
                    try:
                        sociodemos = json_line['events']['profile']['sociodemos']

                        for k, v in sociodemos.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in sociodemos'.format(str(k)))
                                del sociodemos[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in sociodemos'.format(str(v)))
                                del sociodemos[k]
                        current_fields_to_return['sociodemos'] = sociodemos
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning custom_segments ############
                if 'custom_segments' in fields:
                    try:
                        custom_segments = json_line['events']['wam']['custom_segments']

                        for k, v in custom_segments.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in custom_segments'.format(str(k)))
                                del custom_segments[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in custom_segments'.format(str(v)))
                                del custom_segments[k]
                        current_fields_to_return['custom_segments'] = custom_segments
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning techno ############
                if 'techno' in fields:
                    try:
                        techno = json_line['events']['wam']['techno']

                        for k,v in techno.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in techno'.format(str(k)))
                                del techno[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in techno'.format(str(v)))
                                del techno[k]
                        current_fields_to_return['techno'] = techno
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                ############ check & cleaning audiences ############
                if 'audiences' in fields:
                    try:
                        audiences = json_line['events']['wam']['audiences']

                        for k,v in audiences.items():
                            if(re.match(p, str(k)) == False):
                                if self.logger:
                                    self.logger.warning('errore chiave {0} in audiences'.format(str(k)))
                                del audiences[k]
                            if(re.match(p, str(v)) == False):
                                if self.logger:
                                    self.logger.warning('errore valore {0} in audiences'.format(str(v)))
                                del audiences[k]
                        current_fields_to_return['audiences'] = audiences
                    except KeyError as e:
                        pass
                        # if self.logger: self.logger.warning('L\'elemento {0} non ha informazioni per la chiave: {1}'.format(json_line['uid'], e) )

                #fields_to_return.append(current_fields_to_return)
                #current_fields_to_return = {}
                yield current_fields_to_return

            # if self.logger: self.logger.debug('File {0} numero di affiche_w totali in lista {1}'.format(f.name, len(fields_to_return)))

        #return fields_to_return
