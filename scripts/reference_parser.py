# ------------------------------------------------------------------------------
# reference_parser.py
# Based on clinvar_parser.py, further info see https://bbddmasterisciii.github.io/files/clinvar_parser.py
# Copyright © 2019–2023 Eduardo Andrés & José Mª Fernández
# All rights reserved.
#
# This script is licensed under the terms of the Creative Commons Attribution license.
# For a copy, see https://creativecommons.org/licenses/by/4.0/
#
# ------------------------------------------------------------------------------

#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import sys
import os
import sqlite3
import re

CLINVAR_REFERENCE_DEFS = [
    """
CREATE TABLE IF NOT EXISTS reference (
    ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    allele_id INTEGER NOT NULL,
    citation_source VARCHAR(64) NOT NULL,
    citation_id VARCHAR(16) NOT NULL
)
""",
    """
CREATE INDEX all_id ON reference(allele_id)
""",
    """
CREATE INDEX cit_id ON reference(citation_id)
"""
]


def open_clinvar_db(db_file):

    db = sqlite3.connect(db_file)

    cur = db.cursor()
    try:
        cur.execute("PRAGMA FOREIGN_KEYS=ON")
        for tableDecl in CLINVAR_REFERENCE_DEFS:
            cur.execute(tableDecl)
    except sqlite3.Error as e:
        print("An error occurred: {}".format(str(e)), file=sys.stderr)
    finally:
        cur.close()

    return db


def store_clinvar_ref(db, reference_file):
    with open(reference_file, "rt") as ref:
        headerMapping = None  # no need?
        cur = db.cursor()

        with db:
            for line in ref:
                wline = line.rstrip("\n")

                if (headerMapping is None) and (wline[0] == '#'):
                    wline = wline.lstrip("#")
                    columnNames = re.split(r"\t", wline)

                    headerMapping = {}
                    for columnId, columnName in enumerate(columnNames):
                        headerMapping[columnName] = columnId

                else:
                    columnValues = re.split(r"\t", wline)

                    for iCol, vCol in enumerate(columnValues):
                        if len(vCol) == 0 or vCol == "-":
                            columnValues[iCol] = None

                    allele_id = int(columnValues[headerMapping["AlleleID"]])
                    citation_source = columnValues[headerMapping["citation_source"]]
                    citation_id = columnValues[headerMapping["citation_id"]]

                    cur.execute("""
                        INSERT INTO reference(
                            allele_id,
                            citation_source,
                            citation_id)
                        VALUES(?,?,?)
                        """, (allele_id, citation_source, citation_id))

                    ventry_id = cur.lastrowid

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: {0} {{database_file}} {{txt_clinvar_reference_file}}".format(
            sys.argv[0]), file=sys.stderr)
        sys.exit(1)

    db_file = sys.argv[1]
    clinvar_file = sys.argv[2]

    db = open_clinvar_db(db_file)
    store_clinvar_ref(db, clinvar_file)
    db.close()