import argparse
import csv
import pprint


def all_movies(movies):
    pprint.pprint(movies)


def movies_titles_from_year(year, movies):
    for i in movies:
        if i['Year Published'] == year:
            pprint.pprint(i['Title'])


def movies_titles_from_year_lambda(year, movies):
    res = list(filter(lambda x: x['Year Published'] == year, movies))
    res_titles = list(map(lambda x: x['Title'], res))
    pprint.pprint(res_titles)


def average_country_consumption(data):
    res = {}
    for d in data:
        if d['LOCATION'] not in res:
            count = 0
            total = 0
            for dd in data:
                if dd['LOCATION'] == d['LOCATION']:
                    total += float(dd['Value'])
                    count += 1
            avg = total / count
            res[d['LOCATION']] = avg
    pprint.pprint(res)


def number_of_years_per_country_with_consumption_higher_than_thirteen(data):
    res = {}
    for d in data:
        if d['LOCATION'] not in res:
            count = 0
            total = 0
            for dd in data:
                if dd['LOCATION'] == d['LOCATION']:
                    total += 1
                    if float(dd['Value']) > 13.0:
                        count += 1
            value = [total, count]
            res[d['LOCATION']] = value
    pprint.pprint(res)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--f', metavar='path-to-data-file', help='Path to a csv file we want to read.')
    args = parser.parse_args()

    data = []

    with open(args.f, 'rb') as csvfile:
        reader = csv.DictReader(csvfile, delimiter=',')
        next(reader, None)  # skip the headers
        for row in reader:
            data.append(row)

    number_of_years_per_country_with_consumption_higher_than_thirteen(data)


if __name__ == "__main__":
    main()
