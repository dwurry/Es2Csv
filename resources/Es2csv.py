#!/usr/bin/env python2.6

import csv
print csv.__file__
try:
    import unicodecsv as csv
except ImportError:
    import csv

import csv
print csv.__file__

import json
import operator
import os
from collections import OrderedDict
import logging
import sys
import codecs
from datetime import datetime
from elasticsearch import helpers, Elasticsearch



logging.basicConfig(level=logging.DEBUG)

class Json2Csv(object):
    """Process a JSON object to a CSV file"""
    collection = None

    # Better for single-nested dictionaries
    #SEP_CHAR = ', '
    #KEY_VAL_CHAR = ': '
    #DICT_SEP_CHAR = '\r'
    #DICT_OPEN = ''
    #DICT_CLOSE = ''

    # Better for deep-nested dictionaries
    SEP_CHAR = ', '
    KEY_VAL_CHAR = ': '
    DICT_SEP_CHAR = '; '
    DICT_OPEN = '{ '
    DICT_CLOSE = '} '

    rowCount = 0;

    def __init__(self, outline):
        self.rows = []
        self.outfile = None

        if not isinstance(outline, dict):
            raise ValueError('You must pass in an outline for JSON2CSV to follow')
        elif 'map' not in outline or len(outline['map']) < 1:
            raise ValueError('You must specify at least one value for "map"')

        key_map = OrderedDict()
        for header, key in outline['map']:
            splits = key.split('.')
            splits = [int(s) if s.isdigit() else s for s in splits]
            key_map[header] = splits

        self.key_map = key_map
        if 'collection' in outline:
            self.collection = outline['collection']
        else:
            print 'collection rejected'

        print 'completed init'

    def load(self, json_file, outfile):
        self.outfile = outfile
        print 'in load(self, json_file)'
        self.process_each(json.load(json_file))

    def process_each(self, data):
        """Process each item of a json-loaded dict
        """
	print 'in process_each(self, data)'

        if self.collection and self.collection in data:
            data = data[self.collection]

        for d in data:
            logging.info(d)
            self.rows.append(self.process_row(d))

    def process_row(self, item):
        """Process a row of json data against the key map
        """
        row = {}
        #print("Working with item %s " % (item))
        for header, keys in self.key_map.items():
            try:
                #print("header %s " %(header))
                row[header] = reduce(operator.getitem, keys, item)
            except (KeyError, IndexError, TypeError):
                row[header] = None

        #print ("the cvs row is %s" %(row))
        return row

    def make_strings(self):
        str_rows = []
        for row in self.rows:
            str_rows.append({k: self.make_string(val)
                             for k, val in row.items()})
        return str_rows

    def make_string(self, item):
        if isinstance(item, list) or isinstance(item, set) or isinstance(item, tuple):
            return self.SEP_CHAR.join([self.make_string(subitem) for subitem in item])
        elif isinstance(item, dict):
            return self.DICT_OPEN + self.DICT_SEP_CHAR.join([self.KEY_VAL_CHAR.join([k, self.make_string(val)]).encode('utf-8').strip() for k, val in item.items()]) + self.DICT_CLOSE
        else:
            return unicode(item)

    def open_csv(self, filename='output.csv', make_strings=False):
        print "opening {0}".format(filename)
        with codecs.open(filename, 'wb+', 'utf-8') as f:
            writer = csv.DictWriter(f, self.key_map.keys())
            writer.writeheader()
            return writer

    def write_csv_row(self, writer, row):
        self.rowCount = self.rowCount + 1
        try:
            with codecs.open(self.outfile, 'a', 'utf-8') as f:
                writer = csv.DictWriter(f, self.key_map.keys())
                writer.writerow(row)
        except ValueError as e:
            print "Value error({0})".format(e)
            # print "Value error character {0}, position {1}".format(e.object, e.start)
            print "file error({0})".format(writer)
            print "for row {0}".format(self.rowCount)
#            print "{0}".format(row)
#            for i, v in enumerate(row) :
#                row[i] = v.replace(e.object,'')
#            self.write_csv_row(self, writer, row)
        except NameError as e:
            print "Name error({0})".format(e)
        except:
            print ("row failed:  ", sys.exc_info()[0])


    def write_csv(self, filename='output.csv', make_strings=False):
        """Write the processed rows to the given filename
        """
        if (len(self.rows) <= 0):
            raise AttributeError('No rows were loaded')
        if make_strings:
            out = self.make_strings()
        else:
            out = self.rows

        writer = self.open_csv(filename=outfile, make_strings = args.strings)

        for row in out:
            self.write_csv_row(writer, row)

class MultiLineJson2Csv(Json2Csv):

    def load(self, json_file, outfile):
        self.outfile = outfile
        self.process_each(json_file)

    #   Open Write File
    def process_each(self, data, collection=None):
        """Load each line of an iterable collection (ie. file)"""
        writer = self.open_csv(self.outfile, make_strings=args.strings)

        for line in data:
            d = json.loads(line)
            if self.collection in d:
                d = d[self.collection]
            self.rows.append(self.process_row(d))

class ElasticJson2Csv(Json2Csv):

    def load(self, json_file, outfile):
        self.outfile = outfile
        self.process_each(json_file)
        '''Elastic Search Host  '''
        es = Elasticsearch(['telemetry-es1.ava.expertcity.com:9200'])
        ''' Elasticsearch query   '''
        '''get ALL messages from source elasticsearch host (es) in specified index'''
        res = helpers.scan(es, index="collaboration-2015.09.24", query={"query": {"match_all": {}}}, doc_type="logs", scroll="5m")

    #   Open Write File
    def process_each(self, data, collection=None):
        """Load each line of an iterable collection (ie. file)"""
        writer = self.open_csv(self.outfile, make_strings=args.strings)

        for hit in res:
            s = json.dumps(hit["_source"])
            if self.collection in s:
                s = s[self.collection]
            self.write_csv_row(writer, self.process_row(s))

def init_parser():
    import argparse
    parser = argparse.ArgumentParser(description="Converts JSON to CSV")
    parser.add_argument('json_file', type=argparse.FileType('r'),
                        help="Path to JSON data file to load")
    parser.add_argument('key_map', type=argparse.FileType('r'),
                        help="File containing JSON key-mapping file to load")
    parser.add_argument('-e', '--each-line', action="store_true", default=False,
                        help="Process each line of JSON file separately")
    parser.add_argument('-o', '--output-csv', type=str, default=None,
                        help="Path to csv file to output")
    parser.add_argument('-q', '--elastic-query', action = "store_true", default=False,
                        help="Use internal ES host/query to get records")
    parser.add_argument(
        '--strings', help="Convert lists, sets, and dictionaries fully to comma-separated strings.", action="store_true", default=True)

    return parser

if __name__ == '__main__':
    parser = init_parser()
    args = parser.parse_args()

    outfile = args.output_csv
    if outfile is None:
        fileName, fileExtension = os.path.splitext(args.json_file.name)
        outfile = fileName + '.csv'

    key_map = json.load(args.key_map)
    loader = None
    if args.each_line:
        loader = MultiLineJson2Csv(key_map)
    elif args.es_query:
        loader = ElasticJson2Csv(key_map)
    else:
        loader = Json2Csv(key_map)

    loader.load(args.json_file, outfile)


    print 'in init parser before loader.write_csv'

    if args.each_line:
        print""
    else:
        loader.write_csv(filename=outfile, make_strings=args.strings)
