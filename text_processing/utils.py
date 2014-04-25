#!/usr/bin/env python

"""
Helper Functions

Siyuan Guo, Apr 2014
"""

def date2daterange(year):
	"""convert date to date range"""
	if year <= 1839:
		return "pre-1839"
	elif year >= 1840 and year <= 1860:
		return "1840-1860"
	elif year >= 1861 and year <= 1876:
		return "1861-1876"
	elif year >= 1877 and year <= 1887:
		return "1877-1887"
	elif year >= 1888 and year <= 1895:
		return "1888-1895"
	elif year >= 1896 and year <= 1901:
		return "1896-1901"
	elif year >= 1902 and year <= 1906:
		return "1902-1906"
	elif year >= 1907 and year <= 1910:
		return "1907-1910"
	elif year >= 1911 and year <= 1914:
		return "1911-1914"
	elif year >= 1915 and year <= 1918:
		return "1915-1918"
	elif year >= 1919 and year <= 1922:
		return "1919-1922"
	else:
		return "1923-present"


def freq2prob(tfdict):
	"""convert a dictionary of raw frequencies to probabilities"""
	total = sum(tfdict.values())
	return {t:tfdict[t]/total for t in tfdict}


def reshape(dict2d):
	"""
	Reshape a 2D dictionary into list of dictionary (MongoDB-ready format), 
	using root level key as _id.
	"""
	return [dict(dict2d[d], **{u"_id":d}) for d in dict2d]
	
class fakedict(object):
	"""Behave like `defaultdict(lambda:1.0)` without wasting memory to save item"""
	def __getitem__(self, key):
		return 1.0
