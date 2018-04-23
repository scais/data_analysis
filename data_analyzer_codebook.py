import argparse
import csv
import elasticsearch
import re
import pprint
from datetime import datetime


def remove_arbitrary_line_endings(entry):
    entry['Author'] = entry['Author'].replace('\n', ' ')
    entry['Context'] = entry['Context'].replace('\n', ' ')
    entry['Name of the Article/Discourse'] = entry['Name of the Article/Discourse'].replace('\n', ' ')


def roles_to_list(entry):
    entry['Code assigned (role)'] = [x.strip() for x in entry['Code assigned (role)'].split(',')]


def keywords_to_list(entry):
    entry['Key Word Observed'] = [x.strip() for x in entry['Key Word Observed'].split(',')]


def sanitize_date(entry):
    str_date = entry['Date of publication in The Pyongyang Times ']

    date_format_regex = re.compile("\d+\/\d+\/\d+")
    if date_format_regex.search(str_date):
        datetime_object = datetime.strptime(str_date, '%d/%m/%Y')

    date_format_regex = re.compile("(\d+.\d+.\d+) \(Published in PT\)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("published (\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("firstly published (\d+. ?\d+. ?\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d. %m. %Y')

    date_format_regex = re.compile("(\d+. \d+. \d+) published in PT")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d. %m. %Y')

    date_format_regex = re.compile("(\d+. \d+. \d+) treatise")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d. %m. %Y')

    date_format_regex = re.compile("firstly published on (\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("published in PT on ?(\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("published in PT on ?(\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile(".*KCNA.*, (\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("published on (\d+.\d+.\d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    date_format_regex = re.compile("published in PT on (\d+. \d+. \d+)")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d. %m. %Y')

    date_format_regex = re.compile("(\d+.\d+.\d+) published in PT")
    if date_format_regex.search(str_date):
        str_date = date_format_regex.search(str_date).group(1)
        datetime_object = datetime.strptime(str_date, '%d.%m.%Y')

    entry['Date of publication in The Pyongyang Times '] = datetime_object.strftime('%Y-%m-%d')


def sanitize(data):
    for entry in data:
        remove_arbitrary_line_endings(entry)
        roles_to_list(entry)
        keywords_to_list(entry)
        sanitize_date(entry)
        entry['Source'] = 'The Pyongyang Times'


def index_data(data):

    es = elasticsearch.Elasticsearch()

    current_id = 0
    for entry in data:
        current_id = current_id + 1
        es.index(index='nk_dataset', doc_type='article', id=current_id, body={
            'source': entry['Source'],
            'author': entry['Author'],
            'code': entry['Code assigned (role)'],
            'context': entry['Context'],
            'date': entry['Date of publication in The Pyongyang Times '],
            'keyword': entry['Key Word Observed'],
            'name': entry['Name of the Article/Discourse'],
            'page': entry['Page no.']
        })


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', metavar='path-to-data-file', help='Path to a csv file we want to read.')
    args = parser.parse_args()

    data = []

    with open(args.f, 'rb') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=';')
        next(reader, None)  # skip the headers
        for row in reader:
            data.append(row)

    sanitize(data)

    index_data(data)

    pprint.pprint(data)


if __name__ == "__main__":
    main()
