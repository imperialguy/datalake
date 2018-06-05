# Requires Python 3
from io import StringIO
import tempfile
import requests
import csv
import re
import os


def parser(download_url):
    compilation1 = re.compile(
        r'^(.+)(Code-\w+)$')
    compilation2 = re.compile(
        r'^(.+)(OPEN INTEREST:\s+)([0-9,]+)$')

    fid, fname = tempfile.mkstemp()
    with open(fname, 'w') as f:
        resp = requests.get(download_url)
        f.write(resp.text)

    csv_stream = StringIO()
    csv_writer = csv.writer(csv_stream, delimiter=',')
    csv_writer.writerow(['contract', 'open_interest'])

    with open(fname) as f:
        contract, open_interest = None, None
        for line in f.readlines():
            matches1 = re.findall(compilation1, line)
            matches2 = re.findall(compilation2, line)

            if matches1 and len(matches1[0]) == 2:
                contract = matches1[0][0].strip()
            elif matches2 and len(matches2[0]) == 3:
                open_interest = matches2[0][2].strip(
                ).replace(',', '')

            if contract and open_interest:
                csv_writer.writerow([contract, open_interest])
                contract, open_interest = None, None

    os.remove(fname)

    return csv_stream.getvalue()


def main():
    download_url = 'https://www.cftc.gov/dea/futures/deanymesf.htm'
    print(parser(download_url))


if __name__ == '__main__':
    main()
