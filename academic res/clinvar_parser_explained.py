# Esta línea de código se llama shebang, se utiliza para especificar el intérprete para el script,
# en este caso, está especificando que el script debe ejecutarse utilizando python3.8. Normalmente 
# es la primera línea de un script y permite que el script se ejecute directamente, sin tener que 
# specificar el intérprete en la línea de comandos. Se utiliza en sistemas basados en Unix, 
# como Linux y MacOS, para indicar que el archivo es un script y debe ser ejecutado por el intérprete especificado.
# !/usr/bin/env python3.8

# Indica que el archivo está codificado en UTF-8, lo cual permite que el archivo contenga caracteres especiales y no ASCII. 
# Esto es importante para asegurar que los caracteres especiales se manejen correctamente en diferentes sistemas operativos y programas.
# -*- coding: utf-8 -*-

# 'sys' y 'os' son módulos incorporados que proporcionan parámetros y funciones específicos del sistema.
import sys, os

# 'sqlite3' es un módulo que proporciona acceso a bases de datos SQLite.
import sqlite3

# 'gzip' es un módulo que proporciona funciones para comprimir y descomprimir archivos en formato gzip.
import gzip

# 're' es un módulo que proporciona funciones para trabajar con expresiones regulares en Python.
import re


# Este código es una lista de cadenas de texto que contiene comandos SQL para crear tablas en una base de datos SQLite. 
# Cada cadena de texto es un comando SQL separado para crear una tabla diferente.
CLINVAR_TABLE_DEFS = [

# La primera tabla se llama "gene" y tiene tres columnas: "gene_id", "gene_symbol" y "HGNC_ID". 
# La columna "gene_symbol" es la clave principal (La clave principal en SQL es un campo o conjunto 
# de campos en una tabla que se utilizan para identificar de manera única un registro o fila dentro 
# de esa tabla. Es una restricción de la base de datos que garantiza que cada valor en el campo o 
# conjunto de campos es único en toda la tabla. No puede haber valores duplicados en una clave principal. 
# En general, se recomienda utilizar un campo numérico como clave principal ya que son más rápidos de 
# procesar y comparar que los campos de tipo texto.) de la tabla.
"""
CREATE TABLE IF NOT EXISTS gene (
	gene_id INTEGER NOT NULL,
	gene_symbol VARCHAR(64) NOT NULL,
	HGNC_ID VARCHAR(64) NOT NULL,
	PRIMARY KEY (gene_symbol)
)
"""
,

# La segunda tabla se llama "variant" y tiene varias columnas... La columna "ventry_id" es la clave primaria y se autoincrementa.
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
# Después de estas tablas, hay varios comandos para crear índices (Un índice en una tabla SQL es 
# una estructura de datos adicional que se utiliza para mejorar el rendimiento de las consultas. 
# Un índice se crea en una o varias columnas de una tabla, y se utiliza para buscar rápidamente 
# los datos en esas columnas. Los índices se pueden crear de varios tipos, como índices de búsqueda 
# binaria, índices de árbol B, índices de hash, entre otros. Cada tipo de índice tiene sus propias 
# características y se utiliza para diferentes tipos de consultas.) en las tablas "variant" en las 
# columnas "chro_start", "chro_stop", "chro", "assembly" y "gene_symbol".
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
# También hay tablas adicionales creadas: "gene2variant", "clinical_sig", "review_status", "variant_phenotypes" 
# y todas tienen relaciones entre ellas mediante llaves foraneas (Una llave foranea es una columna o conjunto 
# de columnas en una tabla que se relaciona con una columna o conjunto de columnas en otra tabla. La llave foranea 
# es utilizada para establecer una relación entre las dos tablas y para garantizar la integridad referencial en la 
# base de datos. Es decir, que los datos en la tabla que contiene la llave foranea se refieran a una fila existente 
# en la tabla a la cual esta haciendo referencia.
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

# Este código es una función llamada "open_clinvar_db" que se encarga de crear una base de datos SQLite3 
# con las tablas necesarias para almacenar datos de Clinvar o abrirla si ya existe. La función recibe como 
# argumento una variable "db_file" que indica el nombre del archivo de la base de datos.
def open_clinvar_db(db_file):

    # Dentro de la función, primero se conecta a la base de datos con la función "sqlite3.connect"	
	db = sqlite3.connect(db_file)
	
    # Se crea un cursor para poder ejecutar comandos SQL en la base de datos. 
	cur = db.cursor()
	
    	# En Python, la estructura "try-except" se utiliza para controlar errores o excepciones que pueden ocurrir
	# en un bloque de código. El bloque "try" contiene el código que se desea ejecutar y el o los bloques "except" 
	# contienen el código que se ejecutará en caso de que ocurra una excepción específica. Y finalmente, se puede 
	# utilizar un bloque "finally" para ejecutar código independientemente de si ocurre o no una excepción
	try:
		# A continuación, se habilita la verificación de integridad de las claves foraneas en la base 
		# de datos con el comando "PRAGMA FOREIGN_KEYS=ON".
		cur.execute("PRAGMA FOREIGN_KEYS=ON")
		
       	 	# Luego, se utiliza un ciclo "for" para recorrer una lista de definiciones de tablas llamada "CLINVAR_TABLE_DEFS" 
		# y crear cada una de estas tablas en la base de datos si aún no existen.
		for tableDecl in CLINVAR_TABLE_DEFS:
			cur.execute(tableDecl)
	
    	# Este código imprime un mensaje de error en la consola, indicando que un error ha ocurrido al ejecutar el código anterior. 
	# El mensaje de error incluye información sobre el error específico que se ha producido, y se imprime en la consola 
	# de error (sys.stderr) en lugar de la consola estándar (sys.stdout).
	except sqlite3.Error as e:
		print("An error occurred: {}".format(str(e)), file=sys.stderr)
	
    	# Finalmente, se cierra el cursor y se devuelve la conexión a la base de datos.
	finally:
		cur.close()
	
	return db

# La función "store_clinvar_file" tiene como parámetros de entrada una conexión a una base de datos y un archivo comprimido 
# de ClinVar. Esta función se encarga de almacenar los datos del archivo de ClinVar en la base de datos. El código dentro 
# de la función específica cómo se deben procesar y almacenar los datos en la base de datos. Puede incluir acciones 
# como crear tablas, insertar datos, actualizar registros, etc. El objetivo final de esta función es tener los datos del 
# archivo de ClinVar disponibles en la base de datos para su uso posterior.
def store_clinvar_file(db,clinvar_file):
	
    	# "with gzip.open(clinvar_file, "rt",encoding="utf-8") as cf:" se utiliza para abrir el archivo clinvar_file,
	# comprimido con gzip, en modo lectura y texto, y con la codificación utf-8 especificada. El fichero se abre y 
	# se asigna a la variable "cf" y se cerrará automáticamente cuando el bloque de código termine de ejecutarse.
	with gzip.open(clinvar_file,"rt",encoding="utf-8") as cf:
		
        	# La variable "headerMapping" se utiliza para almacenar la correspondencia entre el nombre de columna 
		# y el id de columna en el encabezado del archivo.
		headerMapping = None
		
        	# "known_genes" es un conjunto que se utiliza para almacenar una lista de genes conocidos.
		known_genes = set()
		
        	# "newVCFCoords" es una variable utilizada para indicar si las coordenadas VCF del fichero son nuevas o no.
		newVCFCoords = False
		
        	# La línea cur = db.cursor() se utiliza para crear un cursor de base de datos, lo cual es necesario para 
		# realizar operaciones en la base de datos, como ejecutar consultas y obtener resultados.
		cur = db.cursor()
		
        	# La sentencia with db: se utiliza para establecer un contexto de trabajo con la base de datos, de forma 
		# que al finalizar el bloque de código dentro de él, las operaciones realizadas en la base de datos se 
		# confirmen automáticamente (commit) si no se produjo ninguna excepción.
		with db:
			
            		# La línea for line in cf: es un bucle que itera sobre cada línea del archivo especificado en la 
			# variable cf, que en este caso es un archivo ClinVar.
			for line in cf:
				
                		# La línea wline = line.rstrip("\n") se utiliza para eliminar el carácter de nueva línea 
				# al final de cada línea del archivo.
				wline = line.rstrip("\n")
				
                		# La condición if (headerMapping is None) and (wline[0] == '#'): se utiliza 
				# para detectar si la línea actual es el encabezado del archivo. Si se cumple, 
				# se elimina el carácter "#" al inicio de la línea y se separan los nombres de 
				# columna utilizando la expresión regular re.split(r"\t",wline).
				if (headerMapping is None) and (wline[0] == '#'):
					wline = wline.lstrip("#")
					columnNames = re.split(r"\t",wline)
					
                    			# En caso afirmativo, crea una variable headerMapping que es un diccionario 
					# que asigna el nombre de la columna a su id de columna. 
					headerMapping = {}
					
                    			# A continuación, recorre los nombres de columna y los añade al diccionario 
					# headerMapping como claves y sus respectivos id de columna como valores.
					for columnId, columnName in enumerate(columnNames):
						headerMapping[columnName] = columnId
					
                    			# Por último, comprueba si la columna "PositionVCF" está en el diccionario 
					# headerMapping y establece la variable newVCFCoords en consecuencia. Esto 
					# se hace probablemente para comprobar si el archivo tiene una columna 
					# específica llamada 'PositionVCF' que puede ser utilizada más adelante en el código.
					newVCFCoords = 'PositionVCF' in headerMapping
					
                    			# Este código se está utilizando para leer un archivo de ClinVar en formato VCF y 
					# cargarlo en una base de datos. La variable "newVCFCoords" se utiliza para determinar 
					# si el archivo contiene coordenadas VCF o no. Si el archivo contiene coordenadas VCF, 
					# las columnas "ReferenceAlleleVCF" y "AlternateAlleleVCF" se utilizarán para almacenar 
					# los alelos de referencia y alternativos respectivamente, de lo contrario, se utilizarán
					# las columnas "ReferenceAllele" y "AlternateAllele".
					if newVCFCoords:
						refAlleleCol = headerMapping["ReferenceAlleleVCF"]
						altAlleleCol = headerMapping["AlternateAlleleVCF"]
			
					else:
						refAlleleCol = headerMapping["ReferenceAllele"]
						altAlleleCol = headerMapping["AlternateAllele"]
						
                		# La función "re.split()" es utilizada para dividir la cadena de texto en varias partes 
				# utilizando un patrón de expresión regular, en este caso, el caracter de tabulación. El
				# resultado es una lista de valores que se almacenan en la variable "columnValues".
				else:
					columnValues = re.split(r"\t",wline)
					
					# Este código está recorriendo cada columna (vCol) en cada fila (wline) del archivo de 
					# entrada, y utilizando el método enumerate para tener un índice (iCol) y un valor (vCol)
					# para cada columna. Si el valor de la columna es una cadena vacía o igual a "-", entonces
					# se establece esa columna como None. Es posible que el archivo de entrada contenga celdas
					# vacías o con un guión en lugar de un valor, y esta línea de código está manejando ese caso
					# para que no causen problemas en el procesamiento posterior del código. Esto es importante
					# ya que las bases de datos a menudo no permiten valores nulos en las columnas, y este código
					# está asegurando que todos los valores en las columnas sean válidos para su inserción en la base de datos.
					for iCol, vCol in enumerate(columnValues):
						if len(vCol) == 0 or vCol == "-":
							columnValues[iCol] = None
					
                    			# Este código está creando variables para cada columna del fichero utilizando el diccionario
					# 'headerMapping' para acceder al índice de cada columna del fichero. Los valores de la columna
					# se asignan a la variable correspondiente. Por ejemplo, el valor de la columna "AlleleID"
					# se asigna a la variable "allele_id", y el valor de la columna "Name" se asigna a la variable
					# "name". Esto se hace para cada columna del fichero, creando variables para todos los valores
					# del fichero. También está convirtiendo algunas de las variables como allele_id, variation_id
					# a int para operaciones posteriores.
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
					
                    			# Este código inserta los valores almacenados en las variables allele_id, name, allele_type,
					# dbSNP_id, phenotype_list, gene_id, gene_symbol, HGNC_ID, assembly, chro, chro_start, chro_stop,
					# ref_allele, alt_allele, cytogenetic, y variation_id en la tabla "variant" de una base de datos
					# SQLite. La función "cur.execute" se utiliza para ejecutar una sentencia SQL. La sentencia
					# "INSERT INTO" se utiliza para insertar datos en una tabla, y la cláusula "VALUES" se utiliza
					# para especificar los valores que se van a insertar. Los caracteres "?" son marcadores de posición
					# para los valores que se proporcionarán como una tupla de argumentos en la función ejecutar, esto
					# se conoce como consultas parametrizadas y es útil para evitar ataques de inyección SQL.
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
#### buscar manera de acotar la linea superior

                    			# A la variable ventry_id se le asigna el valor del último identificador de fila
					# generado por el atributo lastrowid del objeto cursor. Este valor suele ser el
					# identificador único asignado a la fila creada por la sentencia INSERT más reciente
					# ejecutada por el cursor. En este caso, es el identificador único de la entrada
					# de variante que se acaba de añadir a la base de datos.
					ventry_id = cur.lastrowid
					
					### Clinical significance
                    			# Esta línea está obteniendo el valor de la columna "ClinicalSignificance" de la lista
					# "columnValues" utilizando el diccionario "headerMapping", donde las claves son los
					# nombres de las columnas y los valores son el índice correspondiente en la lista "columnValues".
					# El valor obtenido se almacena en la variable "significance".
					significance = columnValues[headerMapping["ClinicalSignificance"]]
					
                    			# Este código comprueba si la variable "significance" no es None. Si no es None,
					# divide el valor de "significance" utilizando el carácter "/" como delimitador 
					# mediante la función re.split(). Esto crea una lista de subcadenas que se combinan
					# con la variable "ventry_id" para crear una nueva lista de tuplas, donde cada
					# tupla contiene "ventry_id" y una de las subcadenas del valor "significance" dividido.
					# Esta nueva lista de tuplas se pasa al método cur.executemany(), que se utiliza para
					# ejecutar una sentencia SQL varias veces con diferentes valores de parámetros. La sentencia
					# SQL que se ejecuta es una sentencia INSERT INTO que inserta "ventry_id" y "significance"
					# en la tabla "clinical_sig". Esto permite al script insertar varias filas en la tabla
					# "clinical_sig" con una sola llamada cur.executemany(), teniendo cada fila un único valor
					# "ventry_id" y "significance" de la lista de tuplas.
					if significance is not None:
						prep_sig = [ (ventry_id, sig)  for sig in re.split(r"/",significance) ]
						cur.executemany("""
							INSERT INTO clinical_sig(
								ventry_id,
								significance)
							VALUES(?,?)
						""", prep_sig)
					
					### Review status
                    			# La variable "status_str" se establece en el valor de la columna "ReviewStatus" de la fila
					# de datos actual que se está procesando. Este valor se recupera buscando el índice de la columna
					# "ReviewStatus" en el diccionario headerMapping. El diccionario headerMapping se utiliza para
					# asignar los nombres de las columnas del archivo a su índice correspondiente, de forma que se
					# pueda acceder a los datos por el nombre de la columna en lugar de por el índice. Esto hace que
					# el código sea más legible y menos propenso a errores causados por cambios en el orden de las columnas del archivo.
					status_str = columnValues[headerMapping["ReviewStatus"]]
					# Este código comprueba si la variable "status_str" no es None (es decir, si tiene un valor). 
					# Si lo tiene, divide la cadena en cada aparición de ", " (coma y espacio) y crea una lista 
					# de tuplas donde cada tupla contiene el ventry_id y uno de los status_str divididos. A continuación,
					# utiliza el método executemany() para insertar estas tuplas en la tabla "review_status" de la
					# base de datos, insertando el primer elemento de cada tupla en la columna "ventry_id"
					# y el segundo elemento en la columna "status".
					if status_str is not None:
						prep_status = [ (ventry_id, status)  for status in re.split(r", ",status_str) ]
						cur.executemany("""
							INSERT INTO review_status(
								ventry_id,
								status)
							VALUES(?,?)
						""", prep_status)
					
					### Variant Phenotypes
                    			# Si la variable "variant_pheno_str" contiene un valor, es probable que se esté utilizando
					# para almacenar una cadena de IDs de fenotipos que están asociados con la variante. La cadena
					# puede contener varios ID separados por un delimitador como una coma o una barra oblicua. 
					# El siguiente paso en el código puede ser dividir esta cadena en una lista de ID individuales
					# utilizando una expresión regular y el método re.split(). Es probable que esta lista de ID
					# se utilice para insertar varias filas en una tabla como "variant_phenotype" en una base de
					# datos relacional. Esta tabla puede tener columnas para el ID de entrada de la variante
					# (ventry_id) y el ID del fenotipo (pheno_id) que se utilizarían para vincular la variante
					# al fenotipo o fenotipos específicos a los que está asociada.
					variant_pheno_str = columnValues[headerMapping["PhenotypeIDS"]]
					
                    			# Este código se refiere a la manipulación de una cadena de texto que contiene información sobre
					# los fenotipos asociados a una variante genética. La primera línea comprueba si la variable 
					# "variant_pheno_str" contiene algún valor, si es así, se procede a la siguiente línea.
					if variant_pheno_str is not None:
						# La segunda línea utiliza una expresión regular (re) para dividir la cadena de texto
						# "variant_pheno_str" en una lista utilizando los caracteres "; " o "|" como separadores.
						# La lista resultante se almacena en la variable "variant_pheno_list".
						variant_pheno_list = re.split(r"[;|]",variant_pheno_str)
						
                        			# La tercera línea crea una lista vacía llamada "prep_pheno" que se utilizará en una línea
						#  posterior del código para almacenar información relacionada con los fenotipos.
						prep_pheno = []
						
                        			# Este código está iterando sobre una lista de identificadores de fenotipos (variant_pheno_list)
						# que se han extraído de una cadena de texto (variant_pheno_str) utilizando una expresión regular.
						# En cada iteración, se verifica si la longitud de la cadena de texto actual (variant_pheno) es
						# igual a 0. Si es así, se salta la iteración actual usando "continue" y se pasa a la siguiente
						# iteración. Esto significa que se están excluyendo los identificadores de fenotipos vacíos de la lista.
						for phen_group_id, variant_pheno in enumerate(variant_pheno_list):
							if len(variant_pheno) == 0:
								continue
							
                            				# Este código es una comprobación para verificar si la cadena "variant_pheno" cumple con
							# cierta expresión regular. La expresión regular "^[1-9][0-9]* conditions$" busca una cadena 
							# que comience con uno o más dígitos seguidos de " conditions". Si la cadena "variant_pheno" 
							# cumple con esta expresión regular, se imprime un mensaje de información 
							# "INFO: Long PhenotypeIDs {} {}: {}" y el código continúa su ejecución sin 
							# realizar ninguna otra acción en esta iteración.
							if re.search("^[1-9][0-9]* conditions$", variant_pheno):
								print("INFO: Long PhenotypeIDs {} {}: {}".format(allele_id, assembly, variant_pheno))
								continue
							
                            				# Este código está dividiendo la cadena almacenada en la variable "variant_pheno" 
							# utilizando una expresión regular que coincide con una coma (,). La lista de cadenas 
							# resultante se almacena en la variable "variant_annots". Esto sugiere que el código
							# probablemente está intentando extraer múltiples valores de la cadena "variant_pheno" 
							# que están separados por comas y almacenarlos en una lista para su posterior procesamiento.
							variant_annots = re.split(r",",variant_pheno)
							
                            				# Este código está iterando sobre una lista de anotaciones de fenotipo llamada "variant_annots", 
							# que se obtiene a partir de una cadena de texto anteriormente dividida por comas.
							for variant_annot in variant_annots:
								
                                				# Para cada anotación de fenotipo en la lista, el código está dividiendo la anotación
								# en dos partes utilizando el método "split" con el carácter ":". Esto es para separar
								# el identificador del espacio de nombres (namespace) del identificador de fenotipo.
								phen = variant_annot.split(":")
								
                                				# Luego, si el tamaño de la lista "phen" es mayor a 1, significa que se han
								# encontrado dos partes (espacio de nombres y identificador de fenotipo), y 
								# se asigna cada una a las variables "phen_ns" y "phen_id".
								if len(phen) > 1:
									phen_ns , phen_id = phen[0:2]
									
                                    					# Finalmente, se agrega una nueva tupla a la lista "prep_pheno", con los
									# valores de "ventry_id", "phen_group_id", "phen_ns" y "phen_id", estos 
									# valores son utilizados para una posible inserción en una tabla de la base de datos.
									prep_pheno.append((ventry_id,phen_group_id,phen_ns,phen_id))
									
                                				# El código verifica si la variable "variant_annot" es diferente de "na". Si es así, 
								# imprime un mensaje de depuración en la consola con información sobre el allele_id, 
								# la assembly, el variant_annot, variant_pheno_str y la línea actual en el archivo 
								# que se está procesando. El objetivo de este código es ayudar al desarrollador a 
								# depurar el código y detectar cualquier problema o error con estos valores específicos.
								elif variant_annot != "na":
									print("DEBUG: {} {} {}\n\t{}\n\t{}".format(allele_id,assembly,variant_annot,variant_pheno_str,line),file=sys.stderr)
						
                        			# Se está ejecutando una consulta SQL para insertar los valores en la tabla "variant_phenotypes"
						# utilizando la lista "prep_pheno" como argumento.
						cur.executemany("""
							INSERT INTO variant_phenotypes(
								ventry_id,
								phen_group_id,
								phen_ns,
								phen_id)
							VALUES(?,?,?,?)
						""", prep_pheno)

    		# Este código cierra el objeto cursor 'cur', que se utiliza para ejecutar comandos SQL 
		# en la base de datos. Una vez que se cierra el cursor, ya no se puede utilizar para ejecutar
		# más comandos. Esto se hace normalmente después de que se hayan ejecutado todos los comandos
		# SQL necesarios y se haya actualizado la base de datos con los nuevos datos.	
		cur.close()

# Este código comprueba si el archivo actual se está ejecutando como un script principal (si su nombre es "main"). 
# Si es así, verifica si se han proporcionado al menos 3 argumentos en la línea de comando (el nombre del script
# y dos argumentos adicionales: el nombre del archivo de base de datos y el nombre del archivo de ClinVar comprimido).
# Si no se proporcionaron suficientes argumentos, se imprime un mensaje de uso en la consola de error y el script
# finaliza con un código de salida de 1 (indicando un error).
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
	store_clinvar_file(db,clinvar_file)

    # Finalmente, la conexión a la base de datos se cierra mediante el método "close()".
	db.close()
