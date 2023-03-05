# ------------------------------------------------------------------------------
# civic_parser.py
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

CIVIC_TABLE_DEFS = [
"""
CREATE TABLE IF NOT EXISTS gene (
	variant_id INTEGER PRIMARY KEY, 
	gene_symbol VARCHAR(16) NOT NULL,
	entrez_id INTEGER NOT NULL
)
"""
,
"""
CREATE INDEX gene_entrez_id ON gene(entrez_id)
"""
,
"""
CREATE INDEX gene_sym ON gene(gene_symbol)
"""
,
"""
CREATE TABLE IF NOT EXISTS variant (
	variant_id INTEGER PRIMARY KEY,
	civic_url VARCHAR(40) NOT NULL,
	gene_symbol VARCHAR(10) NULL,
	entrez_id INTEGER NOT NULL,
	variant VARCHAR(128) NOT NULL,
	var_description VARCHAR (4096) NULL, 
	var_groups VARCHAR(64) NULL,
	var_types VARCHAR(64) NULL,
	ref_bases VARCHAR(32) NULL,
	var_bases VARCHAR(32) NULL,
	ensemble INTEGER NULL,
	ref_build VARCHAR(32) NULL,
	chr_1 VARCHAR(8) NULL,
	chr_start INTEGER NULL,
	chr_stop INTEGER NULL,
	representative_transcript VARCHAR(48) NULL,
	chr_2 VARCHAR(8) NULL,
	chr_2_start INTEGER NULL,
	chr_2_stop INTEGER NULL,
	representative_transcript_2 VARCHAR(48) NULL,
	allele_registry_id VARCHAR(64) NULL,
	civic_evidence_score FLOAT NULL,
	civic_assertion_id VARCHAR(32) NULL,
	civic_assertion_url VARCHAR(128) NULL,
	civic_is_flagged VARCHAR(64) NULL,
	clinvar_ids VARCHAR(32) NULL,
	var_alias VARCHAR(48) NULL
)
"""
,
"""
CREATE INDEX IF NOT EXISTS assembly_variant ON variant(ensemble, ref_build)
"""
,
"""
CREATE INDEX IF NOT EXISTS coords_variant ON variant(chr_start,chr_stop,chr_1,representative_transcript)
"""
,
"""
CREATE INDEX IF NOT EXISTS coords_2_variant ON variant(chr_2_start,chr_2_stop,chr_2,representative_transcript_2)
"""
,
"""
CREATE INDEX IF NOT EXISTS gene_symbol_variant ON variant(gene_symbol)
"""
,
"""
CREATE TABLE IF NOT EXISTS hgvs_expressions (
	ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	variant_id INTEGER NULL,
	hgvs_expression VARCHAR(64) NULL,
	FOREIGN KEY (variant_id) REFERENCES gene(variant_id)
		ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (variant_id) REFERENCES variant(variant_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
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
		for tableDecl in CIVIC_TABLE_DEFS:
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
					variant_id = int(columnValues[headerMapping["variant_id"]])
					civic_url = columnValues[headerMapping["variant_civic_url"]]
					gene_symbol = columnValues[headerMapping["gene"]]
					entrez_id = columnValues[headerMapping["entrez_id"]]
					variant = columnValues[headerMapping["variant"]]
					var_description = columnValues[headerMapping["summary"]]
					var_groups = columnValues[headerMapping["variant_groups"]]
					var_types = columnValues[headerMapping["variant_types"]]
					ref_bases = columnValues[headerMapping["reference_bases"]]
					var_bases = columnValues[headerMapping["variant_bases"]]
					ensemble = columnValues[headerMapping["ensembl_version"]]
					ref_build = columnValues[headerMapping["reference_build"]]
					chr_1 = columnValues[headerMapping["chromosome"]]
					chr_start = columnValues[headerMapping["start"]]
					chr_stop = columnValues[headerMapping["stop"]]
					representative_transcript = columnValues[headerMapping["representative_transcript"]]
					chr_2 = columnValues[headerMapping["chromosome2"]]
					chr_2_start = columnValues[headerMapping["start2"]]
					chr_2_stop = columnValues[headerMapping["stop2"]]
					representative_transcript_2 = columnValues[headerMapping["representative_transcript2"]]
					allele_registry_id = columnValues[headerMapping["allele_registry_id"]]
					civic_evidence_score = columnValues[headerMapping["civic_variant_evidence_score"]]
					civic_assertion_id = columnValues[headerMapping["assertion_ids"]]
					civic_assertion_url = columnValues[headerMapping["assertion_civic_urls"]]
					civic_is_flagged = columnValues[headerMapping["is_flagged"]]
					clinvar_ids = columnValues[headerMapping["clinvar_ids"]]
					var_alias = columnValues[headerMapping["variant_aliases"]]
										
					variant_query = """
						INSERT INTO variant(
							variant_id, civic_url, gene_symbol, entrez_id, variant, var_description,
							var_groups, var_types, ref_bases, var_bases, ensemble, ref_build,
							chr_1, chr_start, chr_stop, representative_transcript, chr_2,
							chr_2_start, chr_2_stop, representative_transcript_2, allele_registry_id,
							civic_evidence_score, civic_assertion_id, civic_assertion_url,
							civic_is_flagged, clinvar_ids, var_alias)
						VALUES(
							?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?
						)
					"""
					bindings = (
						variant_id,civic_url,gene_symbol,entrez_id,variant,var_description,
						var_groups,var_types,ref_bases,var_bases,ensemble,ref_build,chr_1,chr_start,
						chr_stop,representative_transcript,chr_2,chr_2_start,chr_2_stop,
						representative_transcript_2,allele_registry_id,civic_assertion_url,
						civic_evidence_score,civic_assertion_id,civic_is_flagged,clinvar_ids,var_alias
					)

					cur.execute(variant_query,bindings)
					
					# Table gene
					variant_id = int(columnValues[headerMapping["variant_id"]])
					gene_symbol = columnValues[headerMapping["gene"]]
					entrez_id = columnValues[headerMapping["entrez_id"]]
					
					cur.execute("""
					INSERT INTO gene(
						variant_id,
						gene_symbol,
						entrez_id)
					VALUES(?,?,?)
					""", (variant_id,gene_symbol,entrez_id))
					
					
					# HGVS
					variant_id = int(columnValues[headerMapping["variant_id"]])
					hgvs_expression = columnValues[headerMapping["hgvs_expressions"]]
					if hgvs_expression is not None:
						prep_hgvs = [ (variant_id, hgvs_expression) for hgvs_expression in re.split(r",",hgvs_expression) ]
						cur.executemany("""
							INSERT INTO hgvs_expressions(
								variant_id,
								hgvs_expression)
							VALUES(?,?)
						""", prep_hgvs)
		
		cur.close()

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Usage: {0} {{database_file}} {{civic_file}}".format(sys.argv[0]), file=sys.stderr)
		sys.exit(1)

	# Only the first and second parameters are considered
	db_file = sys.argv[1]
	civic_file = sys.argv[2]

	# First, let's create or open the database
	db = open_civic_db(db_file)

	# Second
	store_civic_file(db,civic_file)

	db.close()
