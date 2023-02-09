#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import sys
import os

import sqlite3

# For compressed input files
import re

CLINVAR_REFERENCE_DEFS = [
    """
CREATE TABLE IF NOT EXISTS reference (
    ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    allele_id INTEGER NOT NULL,
    variation_id INTEGER NOT NULL,
    rs INTEGER,
    nsv VARCHAR(64),
    citation_source VARCHAR(64),
    citation_id VARCHAR(64)
)
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
                    allele_id = int(columnValues[headerMapping["AlleleID"]])
                    name = columnValues[headerMapping["Name"]]
                    allele_type = columnValues[headerMapping["Type"]]
                    dbSNP_id = columnValues[headerMapping["RS# (dbSNP)"]]
                    phenotype_list = columnValues[headerMapping["PhenotypeList"]]
                    assembly = columnValues[headerMapping["Assembly"]]
                    chro = columnValues[headerMapping["Chromosome"]]
                    chro_start = columnValues[headerMapping["Start"]]
                    chro_stop = columnValues[headerMapping["Stop"]]
                    cytogenetic = columnValues[headerMapping["Cytogenetic"]]
                    variation_id = int(
                        columnValues[headerMapping["VariationID"]])

                    gene_id = columnValues[headerMapping["GeneID"]]
                    gene_symbol = columnValues[headerMapping["GeneSymbol"]]
                    HGNC_ID = columnValues[headerMapping["HGNC_ID"]]
