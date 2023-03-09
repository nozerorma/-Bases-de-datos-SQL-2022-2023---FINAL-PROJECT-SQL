# ------------------------------------------------------------------------------
# civic_evidence_parser.py
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

import sys, os
import sqlite3
import re

CIVIC_EVIDENCE_DEFS = [
"""
CREATE TABLE IF NOT EXISTS evidence (
	evidence_id INTEGER PRIMARY KEY,
	variant_id INTEGER NOT NULL,
	gene_symbol VARCHAR(16) NOT NULL,
	disease VARCHAR(64) NULL,
	doid INTEGER NULL,
	phenotypes VARCHAR(32) NULL,
	evidence_type VARCHAR(16) NOT NULL,
	evidence_direction VARCHAR(32) NULL,
	evidence_level VARCHAR(1) NOT NULL,
	clinical_significance VARCHAR(32) NULL,
	evidence_statement VARCHAR(512) NULL,
	rating INTEGER NULL
)
"""
,
"""
CREATE TABLE IF NOT EXISTS drugs (
	evidence_id INTEGER PRIMARY KEY,
	variant_id INTEGER NOT NULL,
	drugs VARCHAR(64) NULL,
	drug_interaction_type VARCHAR(32) NULL,
	FOREIGN KEY (evidence_id) REFERENCES evidence(evidence_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)"""
,
"""
CREATE TABLE IF NOT EXISTS citations (
	evidence_id INTEGER PRIMARY KEY,
	variant_id INTEGER NOT NULL,
	citation_id INTEGER NOT NULL,
	source varchar(16) NOT NULL,
	asco_id INTEGER NULL,
	citation VARCHAR(128) NOT NULL,
	nct_ids VARCHAR(32) NULL,
	FOREIGN KEY (evidence_id) REFERENCES evidence(evidence_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)"""
]


def open_civic_db(db_file):
	"""
		This method creates a SQLITE3 database with the needed
		tables to store civic data, or opens it if it already
		exists
	"""
	
	db = sqlite3.connect(db_file)
	
	cur = db.cursor()
	try:
		# Let's enable the foreign keys integrity checks
		cur.execute("PRAGMA FOREIGN_KEYS=ON")
		
		# And create the tables, in case they were not previously
		# created in a previous use
		for tableDecl in CIVIC_EVIDENCE_DEFS:
			cur.execute(tableDecl)
	except sqlite3.Error as e:
		print("An error occurred: {}".format(str(e)), file=sys.stderr)
	finally:
		cur.close()
	
	return db
	
def store_civic_file(db,civic_file):
	with open(civic_file,"rt",encoding="utf-8") as cf:
		headerMapping = None

		cur = db.cursor()
		
		with db:
			for line in cf:
				wline = line.rstrip("\n")
				if (headerMapping is None):
					columnNames = re.split(r"\t",wline)
					headerMapping = {}
					for columnId, columnName in enumerate(columnNames):
						headerMapping[columnName] = columnId
				else:
					columnValues = re.split(r"\t",wline)
					
					# As these values can contain "nulls", which are
					# designed as 'N/A', substitute them for None
					for iCol, vCol in enumerate(columnValues):
						if len(vCol) == 0 or vCol == "N/A":
							columnValues[iCol] = None
					
					# Table variation
					#import pdb; pdb.set_trace()
					evidence_id = int(columnValues[headerMapping["evidence_id"]])
					variant_id = int(columnValues[headerMapping["variant_id"]])
					gene_symbol = columnValues[headerMapping["gene"]]
					disease = columnValues[headerMapping["disease"]]
					doid = columnValues[headerMapping["doid"]]
					phenotypes = columnValues[headerMapping["phenotypes"]]
					evidence_type = columnValues[headerMapping["evidence_type"]]
					evidence_direction = columnValues[headerMapping["evidence_direction"]]
					evidence_level = columnValues[headerMapping["evidence_level"]]
					clinical_significance = columnValues[headerMapping["clinical_significance"]]
					evidence_statement = columnValues[headerMapping["evidence_statement"]]
					rating = columnValues[headerMapping["rating"]]
					
					evidence_query = """
						INSERT INTO evidence(
							evidence_id, variant_id, gene_symbol, disease, doid, phenotypes,
							evidence_type, evidence_direction, evidence_level, clinical_significance,
							evidence_statement, rating)
						VALUES(
							?,?,?,?,?,?,?,?,?,?,?,?
						)
					"""
					bindings = (
						evidence_id,variant_id,gene_symbol,disease,doid,phenotypes,evidence_type,
						evidence_direction,evidence_level,clinical_significance,evidence_statement,
						rating
					)

					cur.execute(evidence_query,bindings)
					
					# Table drugs
					evidence_id = int(columnValues[headerMapping["evidence_id"]])
					variant_id = int(columnValues[headerMapping["variant_id"]])
					drugs = columnValues[headerMapping["drugs"]]
					drug_interaction_type = columnValues[headerMapping["drug_interaction_type"]]
					
					cur.execute("""
					INSERT INTO drugs(
						evidence_id,
						variant_id,
						drugs,
						drug_interaction_type)
					VALUES(?,?,?,?)
					""", (evidence_id,variant_id,drugs,drug_interaction_type))
					
					
					# Table citations
					evidence_id = int(columnValues[headerMapping["evidence_id"]])
					variant_id = int(columnValues[headerMapping["variant_id"]])
					citation_id = int(columnValues[headerMapping["citation_id"]])
					source = columnValues[headerMapping["source_type"]]
					asco_id = columnValues[headerMapping["asco_abstract_id"]]
					citation = columnValues[headerMapping["citation"]]
					nct_ids = columnValues[headerMapping["nct_ids"]]
					
					cur.execute("""
					INSERT INTO citations(
						evidence_id,
						variant_id,
						citation_id,
						source,
						asco_id,
						citation,
						nct_ids)
					VALUES(?,?,?,?,?,?,?)
					""", (evidence_id,variant_id,citation_id,source,asco_id,citation,nct_ids))

		cur.close()

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Usage: {0} {{database_file}} {{civic_file}}".format(sys.argv[0]), file=sys.stderr)
		sys.exit(1)

	# Only the first and second parameters are considered
	db_file = sys.argv[1]
	civic_evidence_file = sys.argv[2]

	# First, let's create or open the database
	db = open_civic_db(db_file)

	# Second
	store_civic_file(db,civic_evidence_file)

	db.close()
