#!/usr/bin/env python

"""Adapted from [xml2json.py](https://github.com/hay/xml2json) to
1) keep attribute tag when striping namespace;
2) abandon json2xml functionality;
3) use mongoDB default formatted JSON (one document perline);
4) ONLY WORK FOR HATHITRUST XML METADATA;
"""

"""Original License:
Copyright (C) 2010-2013, Hay Kranen
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
"""

import json, sys, re
import xml.etree.cElementTree as ET

def strip_tag(tag):
    strip_ns_tag = tag
    split_array = tag.split('}')
    if len(split_array) > 1:
        strip_ns_tag = split_array[1]
        tag = strip_ns_tag
    return tag

def elem_to_internal(elem, strip_ns=1, strip=1):
    """Convert an Element into an internal dictionary (not JSON!)."""
    d = {}
    elem_tag = elem.tag
    if strip_ns:
        elem_tag = strip_tag(elem.tag)
    for key, value in list(elem.attrib.items()):
            d['_' + key] = value

    # loop over subelements to merge them
    for subelem in elem:
        v = elem_to_internal(subelem, strip_ns=strip_ns, strip=strip)
        tag = subelem.tag
        if strip_ns:
            tag = strip_tag(subelem.tag)
        value = v[tag]
        try:
            # add to existing list for this tag
            d[tag].append(value)
        except AttributeError:
            # turn existing entry into a list
            d[tag] = [d[tag], value]
        except KeyError:
            # add a new non-list entry
            d[tag] = value
    text = elem.text
    tail = elem.tail
    if strip:
        # ignore leading and trailing whitespace
        if text:
            text = text.strip()
        if tail:
            tail = tail.strip()
    if tail:
        d['#tail'] = tail
    if d:
        # use #text element if other attributes exist
        if text:
            d["#text"] = text
    else:
        # text is the value if no attributes
        d = text or None
    return {elem_tag: d}

def elem2json(elem, strip_ns=1, strip=1):
    """Convert an ElementTree or Element into a JSON string."""
    if hasattr(elem, 'getroot'):
        elem = elem.getroot()
    return json.dumps(elem_to_internal(elem, strip_ns=strip_ns, strip=strip), sort_keys=True)

def xml2json(xmlstring, strip_ns=1, strip=1):
    """Convert an XML string into a JSON string."""
    elem = ET.fromstring(xmlstring)
    jout = elem2json(elem, strip_ns=strip_ns, strip=strip)
    # Note that this json output is in standard json format, not MongoDB default format
    return jout

def main():
    if len(sys.argv) != 3:
        print "Please provide input and output filenames. Input filename first, output filename second."
        sys.exit()
    else:
        infilename,outfilename = sys.argv[1:]
        with open(infilename, 'r') as fin:
            out = xml2json(fin.read(), strip_ns=1, strip=1)
        with open(outfilename, 'w') as fout:
            fout.write(out)

if __name__ == "__main__":
    main()

