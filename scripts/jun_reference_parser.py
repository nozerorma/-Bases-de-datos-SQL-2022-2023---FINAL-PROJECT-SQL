#!/usr/bin/env python3.8
# -*- coding: utf-8 -*-

import sys
import os
import sqlite3
import re
import logging

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

# think about creating table inside def declaration
CLINVAR_REFERENCE_DEFS = [
    """
CREATE TABLE IF NOT EXISTS reference (
    ventry_id INTEGER PRIMARY KEY AUTOINCREMENT,
    allele_id INTEGER NOT NULL,
    citation_source VARCHAR(64) NOT NULL,
    citation_id VARCHAR(16)
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
            logging.error("An error occurred: %s", str(e))
    finally:    
        cur.close()

    return db


def store_clinvar_ref(db, reference_file):
    with open(reference_file, "rt") as ref:
        headerMapping = None # no need?
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
    cur = db.cursor()
    with db:
        cur.execute("""
            SELECT name
            FROM sqlite_master
            WHERE type='table'
            AND name='variant'
        """
        )
        isPresent = cur.fetchone()

        if isPresent:
            #this is a temp solution prior to table curation
            print("Variant table could be fetched from {db_file}")
        
        else:
            print("Variant table could not be fetched from {db_file}")

        cur.close()    


if __name__ == '__main__':
	if len(sys.argv) < 3:
		print("Usage: {0} {{database_file}} {{compressed_clinvar_file}}".format(sys.argv[0]), file=sys.stderr)
		sys.exit(1)

	# La siguiente línea asigna el primer argumento pasado al script a la variable "db_file" 
	# y el segundo argumento a la variable "clinvar_file".
	db_file = sys.argv[1]
	clinvar_file = sys.argv[2]

	# Luego, se llama a la función "open_clinvar_db" pasándole la ruta del archivo de la base de datos. 
	# Esta función abre la conexión con la base de datos y devuelve una conexión.
	db = open_clinvar_db(db_file)

	# La función "store_clinvar_file" se llama con los argumentos "db" y "clinvar_file". 
	# Esta función almacena los datos del archivo "clinvar_file" en la base de datos.
	store_clinvar_ref(db,clinvar_file)

    # Finalmente, la conexión a la base de datos se cierra mediante el método "close()".
	db.close()
