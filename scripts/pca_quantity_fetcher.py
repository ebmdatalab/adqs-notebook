"""A scraper for getting SQU (Standard Quantity Unit) for as many BNF
codes as possible, by combining results for as many months as possibe
going back through time.

From a comment in the XLS:

  Standard Quantity Unit:
  This code indicates the form of the drug and the units in which quantity is measured:
   Code 1  - a unit (e.g. one tablet, capsule, pack, aerosol etc)
   Code 3  - millilitres
   Code 6  - grammes
   Code 0  - individually formulated (unit varies)

"""
from datetime import date
from lxml import html
from multiprocessing import Pool
from progress.bar import Bar
import glob
import os
import pandas as pd
import requests
import requests_cache
import shutil
import urllib
import warnings


# https://stackoverflow.com/a/40846742/559140
warnings.filterwarnings("ignore", message="numpy.dtype size changed")
warnings.filterwarnings("ignore", message="numpy.ufunc size changed")


requests_cache.install_cache('cache')

SQU_LOOKUP = {
    1: 'unit',
    3: 'ml',
    6: 'g',
    0: 'individual'  # (entirely?) homeopathic remedies
}


def build_url_list(host, index):
    """Find all URLs on the page matching `{date}.xls`
    """
    urls = []
    months = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
              "Jul", "Aug", "Sept", "Oct", "Nov", "Dec"]
    for month in months:
        urls.extend(index.xpath(
            "//a[starts-with(text(),'{}') "
            "and contains(@href, 'xls')]/@href".format(month))
        )
    normalised_urls = []
    for url in urls:
        if not url.startswith('http'):
            url = host + url
        normalised_urls.append(url)
    return normalised_urls


def process_file(path):
    APPLIANCE_CLASS = 4
    df = pd.read_excel(path, skiprows=[0])
    # Get all distinct bnf_code/SQU pairs
    squs = set(df[df['Preparation Class'] != APPLIANCE_CLASS].groupby(
        ['BNF Code', 'Standard Quantity Unit']).groups.keys())
    return squs


def fetch_url(url):
    r = requests.get(url)

    path = os.path.join(
        "pca_files", urllib.parse.urlparse(r.url)[2].split('/')[-1])
    path = path.replace('%20', '_')
    if r.status_code == 200:
        with open(path, 'wb') as f:
            r.raw.decode_content = True
            shutil.copyfileobj(r.raw, f)


def fetch_urls():
    host = "https://www.nhsbsa.nhs.uk"
    index_url = (host + "/prescription-data/"
                 "dispensing-data/prescription-cost-analysis-pca-data")
    index = html.fromstring(requests.get(index_url).content)

    urls = build_url_list(host, index)
    try:
        os.mkdir("pca_files")
    except FileExistsError:
        pass

    pool = Pool()
    bar = Bar('Fetching URLs', max=len(urls))
    for i in pool.imap(fetch_url, urls):
        bar.next()
    bar.finish()


def create_csv(today):
    bnf_codes_with_squ = set()
    files = glob.glob("pca_files/PCA*")
    bar = Bar('Processing data', max=len(files))
    for path in files:
        bnf_codes_with_squ.update(process_file(path))
        bar.next()
    df = pd.DataFrame(
        list(bnf_codes_with_squ),
        columns=['bnf_code', 'squ']).set_index('bnf_code')
    df.squ = df.squ.apply(lambda squ: SQU_LOOKUP[squ])
    df.groupby('bnf_code').last()  # pick the most recent, where there are dupes
    df.to_csv("squs_{}.csv".format(today))


def main():
    today = date.today().strftime("%Y_%m_%d")
    fetch_urls()
    create_csv(today)

if __name__ == '__main__':
    main()
