#!/usr/bin/env python3.11
# -*- coding: utf-8 -*-
# Script based on clinvar_parser.py (Copyright © 2019–2023 Eduardo Andrés & José Mª Fernández)

import sys, os

import sqlite3

import gzip

import re

def create_cit_table(database_path):

    db = sqlite3.connect(database_path)

    cur = db.cursor()

    try:
        # cur.execute("PRAGMA FOREIGN_KEYS=ON")
        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS citations(
            allele_id INTEGER NOT NULL,
            citation_source VARCHAR(13) NOT NULL,
            citation_id INTEGER NOT NULL
            )
            """
        )
    except sqlite3.Error as e:
        print("An error occurred: {}".format(str(e)), file=sys.stderr)
    finally:
        cur.close()

    return db

def store_citations(db, citations_file_path):

    headerDic = {}

    with open(citations_file_path, "rt") as references:
        
        cur = db.cursor()
        with db:
            for line in references:
                
                wline = line.rstrip("\n")
                
                if wline[0] == "#":
                    #print(wline)
                    wline = wline.lstrip("#")
                    # print(wline)
                    ColumnNames = re.split(r"\t", wline)
                    # print(ColumnNames)
                    for ColId, ColName in enumerate(ColumnNames):
                        #print(ColId, ColName)
                        headerDic[ColName] = ColId
                    #print(headerDic)
                else:
                    ColumnValues = re.split("\t", wline)
                    for iCol, vCol in enumerate(ColumnValues):
                        if len(vCol) == 0 or vCol == "-":
                            ColumnValues[iCol] = None
                    
                    allele_id = int(ColumnValues[headerDic["AlleleID"]])
                    citation_source = ColumnValues[headerDic["citation_source"]]
                    citation_id = ColumnValues[headerDic["citation_id"]]

                    cur.execute(
                        """
                        INSERT INTO citations(
                            allele_id,
                            citation_source,
                            citation_id)
                        VALUES(?,?,?)
                        """,(allele_id, citation_source, citation_id))

def citation_filtering(db, citations_file_path):
                    
    with open(citations_file_path) as references:
        cur = db.cursor()
        with db:
            try:
                # max in here gives as the last entry in allele_id
                cur.execute(
                    """
                    SELECT MAX(c.allele_id) 
                    FROM variant v 
                    LEFT JOIN citations c
                    ON v.allele_id = c.allele_id
                    """
                )
                last_citation = cur.fetchall()

                #Output from fetchall is a list with tuple elements. To extract number:

                last_citation = last_citation[0]
                last_citation = last_citation[0]

                print(f"Last allele_id with citation crossref: #{last_citation}")
                print("Processing posterior entries.")

                cur.execute(
                    """
                    DELETE FROM citations
                    WHERE allele_id > {last_citation}
                    """
                )
            
            except sqlite3.Error as e:
                print("An error occurred: {}".format(str(e)), file=sys.stderr)
            finally:
                cur.close()

if __name__ == '__main__':
    if len(sys.argv) < 3:
        print("Usage: {0} {{database_file}} {{citations_clinvar_file}}".format(sys.argv[0]), file=sys.stderr)
        sys.exit(1)

	# Only the first and second parameters are considered
    db_file = sys.argv[1]
    clinvar_citations_file = sys.argv[2]

	# First, let's open the database and create citations table
    db = create_cit_table(db_file)

	# Second, store citations
    store_citations(db,clinvar_citations_file)

    # Third, filter citations up to last reference.
    citation_filtering(db, clinvar_citations_file)

    db.close()