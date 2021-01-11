from pyteomics import mgf, mzid
import pandas as pd
import json
from utils.utils import flatten_dict

def iter_entries(iterator, debug: bool = True) -> list:
	debug_print=print
	if debug == False:
		def nothing(s):
			pass
		debug_print=nothing
	debug_print(type(iterator))
	entries = list(iterator)
	debug_print("Length:", len(entries))
	if len(entries) > 0:
		debug_print("Example:")
		debug_print()
		try:
			debug_print(json.dumps(entries[0], indent=4))
		except TypeError:
			debug_print(entries[0])
	return entries

def read_mgf(filename: str, debug: bool = True) -> pd.DataFrame:
	entries = iter_entries(mgf.read(filename), debug=debug)
	extracted_entries = [flatten_dict(entry) for entry in entries]
	return pd.DataFrame(data=extracted_entries)

def extract_features_from_mzid_entry(entry: dict) -> dict:
	result = flatten_dict(entry)

	try:
		flatten_dict(result.pop('SpectrumIdentificationItem')[0], result)
	except KeyError:
		pass

	try:
		flatten_dict(result.pop('PeptideEvidenceRef')[0], result)
	except KeyError:
		pass
	
	return result

def read_mzid(filename: str, debug=True) -> pd.DataFrame:
	entries = iter_entries(mzid.read(filename), debug=debug)
	extracted_entries = [extract_features_from_mzid_entry(entry) for entry in entries]
	return pd.DataFrame(data=extracted_entries)

def read(filename: str, debug=True) -> pd.DataFrame:
	df = None
	if filename.endswith('.mgf'):
		df = read_mgf(filename, debug=debug)
	elif filename.endswith('.mzid'):
		df = read_mzid(filename, debug=debug)
	else:
		raise NotImplementedError
	return df