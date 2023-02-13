#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import sys
import os

import sqlite3

# For compressed input files
import re

# think about creating table inside def declaration
CLINVAR_REFERENCE_DEFS = [
    """
CREATE TABLE IF NOT EXISTS reference (
    ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    allele_id INTEGER NOT NULL,
    citation_source VARCHAR(64),
    citation_id VARCHAR(64)
)
"""
]

# EN PRIMERA INSTANCIA VOY A PSEUDOCOPIAR EL CODIGO DEL PROFE
# CUANDO ENTIENDA QUE PASA, LO HAGO ORIGINAL


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


def store_reference_file(db, reference_file):
    with open(reference_file, "rt") as ref:
        # headerMapping = None # no need?
        # known_genes = set() # no use AON
        cur = db.cursor()

        with db:
            for line in ref:
                # First, let's remove the newline
                wline = line.rstrip("\n")

                # Now, detecting the header
                # no factual need for this, just looks good to take out the hash
                if (headerMapping is None) and (wline[0] == '#'):
                    wline = wline.lstrip("#")
                    columnNames = re.split(r"\t", wline)

                    headerMapping = {}
                    # And we are saving the correspondence of column name and id
                    for columnId, columnName in enumerate(columnNames):
                        headerMapping[columnName] = columnId

                else:
                    # We are reading the file contents
                    columnValues = re.split(r"\t", wline)

                    # As these values can contain "nulls", which are
                    # designed as '-', substitute them for None
                    for iCol, vCol in enumerate(columnValues):
                        if len(vCol) == 0 or vCol == "-":
                            columnValues[iCol] = None

                    # And extracting what we really need
                    # Table variation
                    # change for the required vals
                    # ventry somewhere in here? note that for autoincrement, for_keys must be somewhere declared
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

    def ref_curation(db, reference_file):
    # now, here should go the curation definition
        # for it to work, select from left
        with open(reference_file, "rt") as ref:
            cur = db.cursor()

            with db:
                try:
                    cur.exectue("""
                        SELECT *
                        FROM variant v
                        JOIN references r
                        ON v.allele_id = r.allele_id;
                    """)
                    # marcos introduce SELECT MAX y LEFT JOIN, ver
                    last_citation=cur.fetchall() #marcos
                except:
