# coding=utf-8
import csv
import json
import os.path
from utils.genericUtils import save_list_to_csv

class DmAnalyzer:
	"""Class to Analyze Weborama WCM Datamining

	**beware**: you need to add the path of weborama_datamining_manipulation, e.g.::

	sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), '../shared/lib/weborama_datamining_manipulation'))

	Usage::

		>>> import DmAnalyzer
		>>> dmAnalyzer = DmAnalyzer()
		>>> dmAnalyzer.config = {...}
		>>> dmAnalyzer.logger = logger
		>>> filtered_list = dmAnalyzer.filter_dm_and_return_values(...)

	Public Properties:
		- logger
	Public Methods:
		- filter_dm_and_return_values
		- analyze_files_and_return_affiche_w_set
	"""
	def __init__(self):
		self.name   = 'DmAnalyzer'
		self._config = None
		self._logger = None


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



	def filter_dm_and_return_values(self,
									files_to_analyze,
									filters,
									output):
		"""Filters Weborama WCM Datamining based on filters and returns output as a list

		:param files_to_analyze: list of pathname of files to be analyzed
		:param filters: list of filters ``[{"label" : "Campaign ID", "values" : [233]} ,{"label" : "Creative Label label", "values" : ["Intercut"]}]``
		:param output: list of outputs ``["USER ID", "Campaign ID"]``

		:rtype: list of output requested ``[["aabb", 1234], ["ccdd", 4567], ...]``
        """

		values_to_return = []

		# label delle colonne su cui applicare il filtro
		labels = [el['label'] for el in filters]

		# label delle colonne da cui recuperare i valori di output
		labels_ouput = []
		for c_output in output:
			labels_ouput.append(c_output)

		for f in files_to_analyze:
			try:
				#f = '../shared/asset/1354-barilla/tmp/datamining_1354_2016100301_impressionvisibility.csv'
				csvfile = open(f, 'r')
				if self.logger:
					self.logger.debug("File analizzato: {0}".format(f))
				spamreader = csv.reader(csvfile, delimiter=',')
				#scorro in avanti di una riga il csv prima di iniziare l'analisi, evitando la lettura dell'header.
				next(spamreader)
				# recupero gli indici delle colonne che corrispondono alle label dei filtri e degli output
				coloumn_indexes_for_filtering = self._get_index_requested_fields(f, labels)
				coloumn_indexes_for_output = self._get_index_requested_fields(f, labels_ouput)

				for row in spamreader:
					# counter che tiene il conto di quanti elementi sono stati trovati
					# rispetto la lista dei filtri che è stata ricevuta.
					# il valore di output viene restituito solo se vengono trovati tutti i valori dei filtri
					match_found = 0
					# scorro le colonne di ogni singola riga alla ricerca delle colonne da filtrare
					for index, coloumn_index in enumerate(coloumn_indexes_for_filtering):
						cell_content = row[coloumn_index]
						current_label = labels[index]

						#scorro i filtri per verificare se la colonna attuale
						# corrisponde a una delle colonne da filtrare
						for n_filter in filters:
							if n_filter['label'].strip() == current_label.strip():

								# scorro i valori associati a questo filtro per verificare se la riga
								# soddisfa i filtri richiesti
								for val in n_filter['values']:
									# se il contenuto della cella è uguale a uno dei valori associati
									# a questo filtro
									if cell_content.strip() == str(val).strip():

										match_found = match_found + 1;
										# se questa row soddisfa tutti i filtri in AND
										if len(filters) == match_found:
											#print('soddisfa tutti i filtri ')
											# salvo i valori di output in una lista (diventa una riga nel csv.)
											temp_values = []
											for index, out_col in enumerate(coloumn_indexes_for_output):
												# temp_values = temp_values + row[out_col] if index + 1 == len(coloumn_indexes_for_output) else temp_values + row[out_col] + ', '
												temp_values.append(row[out_col])

											# a sua volta associata a una lista generale
											values_to_return.append(temp_values)
			except Exception as e:
				print(">>> {0}".format(e))

		return values_to_return


	def analyze_files_and_return_affiche_w_set(self, files_to_analyze):
		"""Analyzes Weborama WCM Datamining and returns a set of all affiche_w

		:param files_to_analyze: list of pathname of files to be analyzed
		:rtype: a seto of affiche_w found on the files analyzed"""

		affice_w_set = set()

		#print('{1} Numero di file da analizzare: {0}'.format(len(files_to_analyze), self.name))

		#return
		for f in files_to_analyze:
			#print(f)
			try:
				csvfile = open(f, 'r')
				spamreader = csv.reader(csvfile, delimiter=',')
				for row in spamreader:
					#print(', '.join(row))
					try:
						#affiche_w
						affiche_w = row[0]
						affice_w_set.add(affiche_w)
						#colonna optional parameters
						#string_to_json = json.loads(row[24])
					except ValueError as e:

						continue
					except KeyError as e:
						continue#print('{1} {0} non valorizzato'.format(e, self.name))
			except FileNotFoundError:
				if self.logger:
					self.logger.warning('{1} File {0} non trovato'.format(f, self.name))

		return affice_w_set


	def _get_index_requested_fields(self, file_to_analyze, label_requested_fields):
		"""
		Analyzes Weborama WCM Datamining searches for labels passed as Parameters
		and returns indexes of the labels
		first label has index 0

        Parameters
		----------
		file_to_analyze 		: str
			pathname of file to be analyzed
		label_requested_fields : list

		Returns a list
		-------

        """

		index_requested_fields = []

		try:
			csvfile = open(file_to_analyze, 'r')
			spamreader = csv.reader(csvfile, delimiter=',')
			csvfile_header = next(spamreader)

			for label_field_requested in label_requested_fields:
				try:
					index_requested_fields.append(csvfile_header.index(label_field_requested))
				except ValueError as err:
					if self.logger:
						self.logger.warning('{1} Header del File {0} non contiene questo campo {2}'.format(file_to_analyze, self.name, label_field_requested))
					pass

		except ValueError as err2:
			if self.logger:
				self.logger.warning('{1} File {0} non ha header'.format(file_to_analyze, self.name))
			pass



		return index_requested_fields
