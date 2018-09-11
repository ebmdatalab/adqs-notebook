#!/usr/bin/env python

from pathlib import Path
from datetime import date
from lib.pca_quantity_fetcher import main as pca_quantity_fetcher_main
import csv
import os
import pandas as pd
import psycopg2


latest_raw_prescribing_data = 'tmp_eu.raw_prescribing_data_2018_06'


def _sql_dir():
    return Path(__file__).parent / 'sql'


def calculated_adqs_csv(today):
    """Generate a CSV of ADQs back-calculated from monthly prescribing
    data

    """
    with open(os.path.join(_sql_dir(), "calculated_adqs.sql"), "r") as f:
        sql = f.read().format(latest_raw_prescribing_data)
    df = pd.io.gbq.read_gbq(
        sql, 'ebmdatalab', dialect='standard')
    df.to_csv("provided_adqs_{}.csv".format(today))


def product_details_csv(today):
    """Generate a CSV of BNF codes and their corresponding ingredients and
    form indications

    """
    with open(os.path.join(_sql_dir(), "product_details.sql"), "r") as f:
        sql = f.read().format(latest_raw_prescribing_data)
    conn = psycopg2.connect(host="largeweb2.ebmdatalab.net",
                            dbname="prescribing",
                            user="prescribing_readonly",
                            password=os.environ['DB_PASS'])
    cursor = conn.cursor()
    cursor.execute(sql)
    column_names = [desc[0] for desc in cursor.description]
    with open("products_{}.csv".format(today), "w") as f:
        w = csv.writer(f)
        w.writerow(column_names)
        for record in cursor:
            w.writerow(record)


if __name__ == '__main__':
    today = date.today().strftime("%Y_%m_%d")
    print("Extracting product details from dmd in postgres")
    product_details_csv(today)
    print("Extracting ADQs from prescribing data in bigquery")
    calculated_adqs_csv(today)

    # retrying to do OperationNotComplete
    print("Scraping SQU from dispensing data")
    pca_quantity_fetcher_main()
