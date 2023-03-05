# ------------------------------------------------------------------------------
# gene_stats_parser.py
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
import gzip
import re

CLINVAR_STATS_DEFS = [
"""
CREATE TABLE IF NOT EXISTS gene_stats (
    ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    gene_symbol VARCHAR(32) NOT NULL,
    geneID INTEGER NOT NULL,
    total_submissions INTEGER NOT NULL,
    total_alleles INTEGER NULL,
    submissions_reporting_gene INTEGER NULL,
    allele_pathogenicity INTEGER NULL,
    mim_no INTEGER NULL,
    uncertain_no INTEGER NULL,
    conflict_no INTEGER NULL
)
""",
"""
CREATE INDEX gene_report ON gene_stats(submissions_reporting_gene)
""",
"""
CREATE INDEX all_patog ON gene_stats(allele_pathogenicity)
""",
"""
CREATE INDEX mim_number ON gene_stats(mim_no)
"""
]

def open_clinvar_db(db_file):
	db = sqlite3.connect(db_file)

	cur = db.cursor()
	try:
		cur.execute("PRAGMA FOREIGN_KEYS=ON")
		for tableDecl in CLINVAR_STATS_DEFS:
			cur.execute(tableDecl)
	except sqlite3.Error as e:
		print("An error occurred: {}".format(str(e)), file=sys.stderr)
	finally:
		cur.close()

	return db

def store_clinvar_stats(db, stats_file):
    with gzip.open(stats_file, "rt", encoding="utf-8") as sf:
        headerMapping = None
        # Skip first line from the file
        next(sf)
        cur = db.cursor()

        with db:
            for line in sf:
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
			        
                    gene_symbol = columnValues[headerMapping["Symbol"]]
                    geneID = int(columnValues[headerMapping["GeneID"]])
                    total_submissions = int(columnValues[headerMapping["Total_submissions"]])
                    total_alleles = columnValues[headerMapping["Total_alleles"]]
                    submissions_reporting_gene = columnValues[headerMapping["Submissions_reporting_this_gene"]]
                    allele_pathogenicity = columnValues[headerMapping["Alleles_reported_Pathogenic_Likely_pathogenic"]]
                    mim_no = columnValues[headerMapping["Gene_MIM_number"]]
                    uncertain_no = columnValues[headerMapping["Number_uncertain"]]
                    conflict_no = columnValues[headerMapping["Number_with_conflicts"]]

                    cur.execute("""
                        INSERT INTO gene_stats(
                            gene_symbol,
                            geneID,
                            total_submissions,
                            total_alleles,
                            submissions_reporting_gene,
                            allele_pathogenicity,
                            mim_no,
                            uncertain_no,
                            conflict_no)
                        VALUES(?,?,?,?,?,?,?,?,?)
                        """, (gene_symbol, geneID, total_submissions, total_alleles, \
                            submissions_reporting_gene, allele_pathogenicity, mim_no, \
                            uncertain_no, conflict_no)
                    )

                    ventry_id = cur.lastrowid

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: {0} {{database_file}} {{compressed_clinvar_stats_file}}".format(
            sys.argv[0]), file=sys.stderr)
        sys.exit(1)

    db_file = sys.argv[1]
    clinvar_file = sys.argv[2]

    db = open_clinvar_db(db_file)
    store_clinvar_stats(db, clinvar_file)
    db.close()
