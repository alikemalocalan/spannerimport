#!/usr/bin/env python2
#
# This application imports a csv file into a Google Cloud Spanner table
import argparse
import base64
import csv

import pytz
from django.utils.dateparse import parse_datetime
from google.cloud import spanner


class Field(object):
    def __init__(self, col_index, name, type_col):
        self.colIndex = col_index
        self.name = name
        self.type_col = type_col


def insert_data(instance_id, database_id, table_id, batch_size, data_file, format_file, header_status):
    header = False
    if header_status != "false":
        header = True

    """Inserts sample data into the given database.

    The database and table must already exist and can be created using
    `create_database`.
    """
    spanner_client = spanner.Client()
    instance = spanner_client.instance(instance_id)
    database = instance.database(database_id)

    fmtfile = open(format_file, 'r')
    fmtreader = csv.reader(fmtfile)
    fmttypes = []
    icols = 0
    for col in fmtreader:
        fmttypes.append(Field(icols, col[1], col[2]))
        icols = icols + 1

    ifile = open(data_file, "r")
    reader = csv.reader(ifile, delimiter=';')
    alist = []
    irows = 0

    readedcolumns = []

    for i, row in enumerate(reader):

        if (i == 0) & header:  # read header line

            readedcolumns = row
        else:
            for x, colname in enumerate(readedcolumns):

                for fmtname in fmttypes:

                    if colname == fmtname.name:

                        if fmtname.type_col != 'string':

                            if fmtname.type_col == 'integer':
                                if row[x]:
                                    row[x] = int(row[x])
                                else:
                                    row[x] = None

                            elif fmtname.type_col == 'boolean':
                                row[x] = bool(row[x])

                            elif fmtname.type_col == 'float':
                                if row[x]:
                                    row[x] = float(row[x])
                                else:
                                    row[x] = None

                            elif fmtname.type_col == 'timestamp':
                                if row[x]:
                                    row[x] = parse_datetime(row[x]).astimezone(pytz.utc).isoformat('T').replace(
                                        '+00:00', 'Z')
                                else:
                                    row[x] = None

                            else:  # fmtname.typeCol == 'bytes':
                                row[x] = base64.b64encode(row[x])
            alist.append(row)
            irows = irows + 1

    ifile.close()
    rowpos = 0
    batchrows = int(batch_size)

    try:
        while rowpos < irows:
            with database.batch() as batch:
                batch.insert_or_update(
                    table=table_id,
                    columns=readedcolumns,
                    values=alist[rowpos:rowpos + batchrows]
                )
            rowpos = rowpos + batchrows

        print 'inserted {0} rows'.format(rowpos)
    except Exception as e:
        print(e)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument(
        '--instance_id', help='Your Cloud Spanner instance ID.')
    parser.add_argument(
        '--database_id', help='Your Cloud Spanner database ID.',
        default='example_db')
    parser.add_argument(
        '--table_id', help='your table name')
    parser.add_argument(
        '--batchsize', help='the number of rows to insert in a batch')
    parser.add_argument(
        '--data_file', help='the csv input data file')
    parser.add_argument('--format_file', help='the format file describing the input data file')

    parser.add_argument('--headerstatus', help='CSV header true or false')

    args = parser.parse_args()

    insert_data(args.instance_id, args.database_id, args.table_id, args.batchsize, args.data_file, args.format_file,
                args.headerstatus)
