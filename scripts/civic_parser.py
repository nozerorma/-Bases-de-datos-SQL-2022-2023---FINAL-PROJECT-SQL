# ------------------------------------------------------------------------------
# civic_parser.py
# Based on clinva
# r_parser.py, further info see https://bbddmasterisciii.github.io/files/clinvar_parser.py
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
	entrez_id INTEGER NOT NULL,
	
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
	ensemble INTEGER NOT NULL,
	ref_build VARCHAR(32) NOT NULL,
	chr_1 INTEGER NULL,
	chr_start INTEGER NULL,
	chr_stop INTEGER NULL,
	representative_transcript VARCHAR(48) NULL,
	chr_2 INTEGER NULL,
	chr_2_start INTEGER NULL,
	chr_2_stop INTEGER NULL,
	representative_transcript_2 VARCHAR(48) NULL
	allele_registry_id VARCHAR(64) NULL,
	civic_evidence_score INTEGER NOT NULL,
	civic_assertion_id INTEGER NULL,
	civic_assertion_url VARCHAR(128) NULL,
	civic_is_flagged VARCHAR(64) NULL,
	clinvar_ids INTEGER NULL,
	var_alias VARCHAR(48) NULL
)
"""
,
"""
CREATE INDEX assembly_variant ON variant(ensemble, ref_build)
"""
,
"""
CREATE INDEX coords_variant ON variant(chr_start,chr_stop,chr_1,representative_transcript)
"""
,
"""
CREATE INDEX coords_2_variant ON variant(chro_2_start,chro_2_stop,chr_2,representative_transcript_2)
"""
,
"""
CREATE INDEX gene_symbol_variant ON variant(gene_symbol)
"""
,
"""
CREATE TABLE IF NOT EXISTS hgvs_expressions (
	variant_id INTEGER PRIMARY KEY,
	hgvs_expression VARCHAR(64) NULL,
	FOREIGN KEY (variant_id) REFERENCES gene(variant_id)
		ON DELETE CASCADE ON UPDATE CASCADE,
	FOREIGN KEY (variant_id) REFERENCES variant(variant_id)
		ON DELETE CASCADE ON UPDATE CASCADE
)
"""
,
"""
CREATE TABLE IF NOT EXISTS review_status
	variant_id INTEGER PRIMARY KEY,

	"""
]

def open_clinvar_db(db_file):
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
				if (headerMapping is None) and (wline[0] == '#'):
					wline = wline.lstrip("#")
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
					
					# And extracting what we really need
					# Table variation
					variant_id = int(columnValues[headerMapping["VariationID"]])
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
	store_civic_file(db,clinvar_file)

	db.close()