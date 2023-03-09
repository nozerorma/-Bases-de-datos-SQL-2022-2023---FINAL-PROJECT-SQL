# ------------------------------------------------------------------------------
# clinvar_parser.py, further info see https://bbddmasterisciii.github.io/files/clinvar_parser.py
# Copyright © 2019–2023 Eduardo Andrés & José Mª Fernández
# All rights reserved.
#
# This script is licensed under the terms of the Creative Commons Attribution license.
# For a copy, see https://creativecommons.org/licenses/by/4.0/
#
# ------------------------------------------------------------------------------

#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

# Module import, left out os as it isn't used

import sys
import sqlite3
import gzip
import re

# SQL tables declaration
# Different tables where used for different strata of information
# Integrity mantainance

CLINVAR_TABLE_DEFS = [
"""
CREATE TABLE IF NOT EXISTS gene (
	gene_id INTEGER NOT NULL,
	gene_symbol VARCHAR(64) NOT NULL,
	HGNC_ID VARCHAR(64) NOT NULL,
	PRIMARY KEY (gene_symbol)
)
"""
,
"""
CREATE TABLE IF NOT EXISTS variant (
	ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
	allele_id INTEGER NOT NULL,
	name VARCHAR(256),
	type VARCHAR(256) NOT NULL,
	dbSNP_id INTEGER NOT NULL,
	phenotype_list VARCHAR(4096),
	gene_id INTEGER,
	gene_symbol VARCHAR(64),
	HGNC_ID VARCHAR(64),
	assembly VARCHAR(16),
	chro VARCHAR(16) NOT NULL,
	chro_start INTEGER NOT NULL,
	chro_stop INTEGER NOT NULL,
	ref_allele VARCHAR(4096),
	alt_allele VARCHAR(4096),
	cytogenetic VARCHAR(64),
	variation_id INTEGER NOT NULL
)
"""
,
"""
CREATE INDEX coords_variant ON variant(chro_start,chro_stop,chro)
"""
,
"""
CREATE INDEX assembly_variant ON variant(assembly)
"""
,
"""
CREATE INDEX gene_symbol_variant ON variant(gene_symbol)
"""
,
"""
CREATE TABLE IF NOT EXISTS gene2variant (
	gene_symbol VARCHAR(64) NOT NULL,
	ventry_id INTEGER NOT NULL,
	PRIMARY KEY (ventry_id, gene_symbol),
	FOREIGN KEY (gene_symbol) REFERENCES gene(gene_symbol)
		ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (ventry_id) REFERENCES variant(ventry_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
,
"""
CREATE TABLE IF NOT EXISTS clinical_sig (
	ventry_id INTEGER NOT NULL,
	significance VARCHAR(64) NOT NULL,
	FOREIGN KEY (ventry_id) REFERENCES variant(ventry_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
,
"""
CREATE TABLE IF NOT EXISTS review_status (
	ventry_id INTEGER NOT NULL,
	status VARCHAR(64) NOT NULL,
	FOREIGN KEY (ventry_id) REFERENCES variant(ventry_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
,
"""
CREATE TABLE IF NOT EXISTS variant_phenotypes (
	ventry_id INTEGER NOT NULL,
	phen_group_id INTEGER NOT NULL,
	phen_ns VARCHAR(64) NOT NULL,
	phen_id VARCHAR(64) NOT NULL,
	FOREIGN KEY (ventry_id) REFERENCES variant(ventry_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
]

# Clinvar file open function
def open_clinvar_db(db_file):
	"""
		This method creates a SQLITE3 database with the needed
		tables to store clinvar data, or opens it if it already
		exists
	"""
	# SQLite3 connection
	db = sqlite3.connect(db_file)
	
	cur = db.cursor()
	try:
		# Foreign keys integrity checks
		cur.execute("PRAGMA FOREIGN_KEYS=ON")
		
		# Table declaration
		for tableDecl in CLINVAR_TABLE_DEFS:
			cur.execute(tableDecl)
	
	# Exception prompt if no table is to be declared
	except sqlite3.Error as e:
		print("An error occurred: {}".format(str(e)), file=sys.stderr)
	
	# Close cursor
	finally:
		cur.close()
	
	return db

# Main data input function	
def store_clinvar_file(db,clinvar_file):
	
	# Open file in gzip format, stablish encoding
	with gzip.open(clinvar_file,"rt",encoding="utf-8") as cf:
		
		# Set header as none, in order to map it afterwards
		headerMapping = None
		
		# This variable teaches the code that the file being
		# parsed has new VCF coordinate, reference and alternate
		# allele columns
		newVCFCoords = False
		
		cur = db.cursor()
		
		with db:
			for line in cf:
				# First, let's remove the newline
				wline = line.rstrip("\n")
				
				# Now, detecting the header
				if (headerMapping is None) and (wline[0] == '#'):
					wline = wline.lstrip("#")
					columnNames = re.split(r"\t",wline)
					
					headerMapping = {}
					# And we are saving the correspondence of column name and id
					for columnId, columnName in enumerate(columnNames):
						headerMapping[columnName] = columnId
					
					newVCFCoords = 'PositionVCF' in headerMapping
					if newVCFCoords:
						refAlleleCol = headerMapping["ReferenceAlleleVCF"]
						altAlleleCol = headerMapping["AlternateAlleleVCF"]
						# 'PositionVCF' might be more important than 'Start'
						# but the program is ignoring it for now
					else:
						refAlleleCol = headerMapping["ReferenceAllele"]
						altAlleleCol = headerMapping["AlternateAllele"]
				else:
					# We are reading the file contents	
					columnValues = re.split(r"\t",wline)
					
					# As these values can contain "nulls", which are
					# designed as '-', substitute them for None
					for iCol, vCol in enumerate(columnValues):
						if len(vCol) == 0 or vCol == "-":
							columnValues[iCol] = None
					
					# And extracting what we really need
					# Table variation
					allele_id = int(columnValues[headerMapping["AlleleID"]])
					name = columnValues[headerMapping["Name"]]
					allele_type = columnValues[headerMapping["Type"]]
					dbSNP_id = columnValues[headerMapping["RS# (dbSNP)"]]
					phenotype_list = columnValues[headerMapping["PhenotypeList"]]
					assembly = columnValues[headerMapping["Assembly"]]
					chro = columnValues[headerMapping["Chromosome"]]
					chro_start = columnValues[headerMapping["Start"]]
					chro_stop = columnValues[headerMapping["Stop"]]
					ref_allele = columnValues[refAlleleCol]
					alt_allele = columnValues[altAlleleCol]
					cytogenetic = columnValues[headerMapping["Cytogenetic"]]
					variation_id = int(columnValues[headerMapping["VariationID"]])
					
					gene_id = columnValues[headerMapping["GeneID"]]
					gene_symbol = columnValues[headerMapping["GeneSymbol"]]
					HGNC_ID = columnValues[headerMapping["HGNC_ID"]]
					
					#if assembly is None:
					#	print("DEBUGAN: "+line,file=sys.stderr)
					#	sys.stderr.flush()
					#	#continue
					
					
					cur.execute("""
						INSERT INTO variant(
							allele_id,
							name,
							type,
							dbsnp_id,
							phenotype_list,
							gene_id,
							gene_symbol,
							hgnc_id,
							assembly,
							chro,
							chro_start,
							chro_stop,
							ref_allele,
							alt_allele,
							cytogenetic,
							variation_id)
						VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
					""", (allele_id,name,allele_type,dbSNP_id,phenotype_list,gene_id,gene_symbol,HGNC_ID,assembly,chro,chro_start,chro_stop,ref_allele,alt_allele,cytogenetic,variation_id))
					
					# The autoincremented value is got here
					### WTF is going on here
					ventry_id = cur.lastrowid
					
					## Table gene
					#gene_id = columnValues[headerMapping["GeneID"]]
					#gene_symbol = columnValues[headerMapping["GeneSymbol"]]
					#HGNC_ID = columnValues[headerMapping["HGNC_ID"]]
					#
					#if gene_id not in known_genes:
					#	cur.execute("""
					#		INSERT INTO gene(
					#			gene_id,
					#			gene_symbol,
					#			hgnc_id)
					#		VALUES(?,?,?)
					#	""", (gene_id,gene_symbol,HGNC_ID))
					#	known_genes.add(gene_id)
					
					# Clinical significance
					significance = columnValues[headerMapping["ClinicalSignificance"]]
					if significance is not None:
						prep_sig = [ (ventry_id, sig)  for sig in re.split(r"/",significance) ]
						cur.executemany("""
							INSERT INTO clinical_sig(
								ventry_id,
								significance)
							VALUES(?,?)
						""", prep_sig)
					
					# Review status
					### GOTTA REVISE WHAT THIS **executemany** does
					status_str = columnValues[headerMapping["ReviewStatus"]]
					if status_str is not None:
						prep_status = [ (ventry_id, status)  for status in re.split(r", ",status_str) ]
						cur.executemany("""
							INSERT INTO review_status(
								ventry_id,
								status)
							VALUES(?,?)
						""", prep_status)
					
					# Variant Phenotypes
					variant_pheno_str = columnValues[headerMapping["PhenotypeIDS"]]
					if variant_pheno_str is not None:
						variant_pheno_list = re.split(r"[;|]",variant_pheno_str)
						prep_pheno = []
						for phen_group_id, variant_pheno in enumerate(variant_pheno_list):
							if len(variant_pheno) == 0:
								continue
							if re.search("^[1-9][0-9]* conditions$", variant_pheno):
								print("INFO: Long PhenotypeIDs {} {}: {}".format(allele_id, assembly, variant_pheno))
								continue
							variant_annots = re.split(r",",variant_pheno)
							for variant_annot in variant_annots:
								phen = variant_annot.split(":")
								if len(phen) > 1:
									phen_ns , phen_id = phen[0:2]
									prep_pheno.append((ventry_id,phen_group_id,phen_ns,phen_id))
								elif variant_annot != "na":
									print("DEBUG: {} {} {}\n\t{}\n\t{}".format(allele_id,assembly,variant_annot,variant_pheno_str,line),file=sys.stderr)
						
						cur.executemany("""
							INSERT INTO variant_phenotypes(
								ventry_id,
								phen_group_id,
								phen_ns,
								phen_id)
							VALUES(?,?,?,?)
						""", prep_pheno)
		
		cur.close()

if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Usage: {0} {{database_file}} {{compressed_clinvar_file}}".format(sys.argv[0]), file=sys.stderr)
		sys.exit(1)

	# Only the first and second parameters are considered
	db_file = sys.argv[1]
	clinvar_file = sys.argv[2]

	# First, let's create or open the database
	db = open_clinvar_db(db_file)

	# Second
	store_clinvar_file(db,clinvar_file)

	db.close()
