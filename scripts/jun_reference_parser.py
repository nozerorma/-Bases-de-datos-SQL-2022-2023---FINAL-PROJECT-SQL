#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import sys, os

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

def store_reference_file(db,reference_file):
    with reference_file as ref:
        headerMapping = None
        #known_genes = set() #no use AON
        cur = db.cursor()