# MEMORIA TRABAJO SQL

## Realizada por Miguel Ramón Alonso para el módulo de SQL del Máster de Bioinformática

###### Organizado por el ISCIII en colaboración con el BSC y otras instituciones

### INTRODUCCIÓN

El objetivo de este trabajo consistió en aprender a trabajar con bases de datos relacionales SQL, utilizando la libreria OSS SQLite. Para la creación y modificación de dichas bases de datos, se propuso utilizar diversos scripts escritos en Python, los cuales se encargarían de relacionar la información contenida en los diversos ficheros almacenados en los históricos de ClinVar y CIViC requeridos para la realización de la práctica. Específicamente, se requirió el procesado de la base de datos de variantes de ClinVar, en sus versiones congeladas de junio y diciembre de 2022, mediante el script *clinvar_parser.py*; a continuación, debíamos escribir dos scripts más, basados en éste, los cuales se encargaran de añadir a la BBDD creada por dicho script la información hallada en los ficheros de citaciones y de estadísticas específicas de cada gen.  Paralelamente, debíamos realizar un script similar al modelo utilizado en clase para el parseo de la base de datos de variantes contenida en CIViC. 

### PRIMERA PARTE

#### 1) Carga de fichero de referencias de ClinVar en la BBDD generada por *clinvar_parser.py*

Misma lógica programática seguida en el script *clinvar_parser.py*, creando una nueva tabla específica para la carga de las citaciones contenidas en el fichero *var_cditations.txt*, y simplificando el código.

#### 2) Carga de fichero de estadísticas por gen de ClinVar en la BBDD generada por *clinvar_parser.py*

Igual que el anterior, introduciendo el siguiente bloque de código (ln 63-67) para procesar la primera línea del fichero: 

```python
 with gzip.open(stats_file, "rt", encoding="utf-8") as sf:
        headerMapping = None
        # remove first line of the file
        next(sf)
        cur = db.cursor()
```

En este caso, el fichero utilizado fue *gene_specific_summary_2022-(mm).txt.gz*, en sus versiones congeladas de junio y de septiembre de 2022.

#### 3) Elaboración de BBDD de CIViC a partir del fichero *01-(MMM)-2022-VariantSummaries.tsv*

En este tercer caso, también se tomo como referencia el script anteriormente referido. Debido a la estructura del fichero *...VariantSummaries.tsv*, se decidió purgar gran cantidad de código no útil en el procesamiento de este fichero. Así mismo, se crearon tres tablas: una *(gene)* con información sobre el gen específico y sus respectivas entradas modelo **entrez_id**; en otra tabla *(variant)* se introdujo el grueso de la información contenida en la base de datos de CIViC; y una tercera en la que se incluyeron las expresiones HGVS de manera más ordenada. También se indexaron varias claves de las tablas para optimizar su acceso durante la realización de la segunda parte del ejercicio.

A diferencia de los datos de ClinVar, en CIViC, las celdas que no tienen información vienen asignadas con N/A; esto debe ser reflejado en el código para realizar correctamente el cambio a NULL a la hora de insertar los datos en la BBDD.

#### 3.5) Elaboración de BBDD de CIViC a partir del fichero *01-(MMM)-2022-VariantSummaries.tsv*

Para una mayor integridad de los datos, se decidió realizar un script adicional conteniendo la información de referencias y evidencia, "a la" ClinVar. De esta manera, mantuve una mayor lógica estructural en mi cabeza a la hora de entender qué estaba haciendo. El script se calcó al de CIViC original, modificando los datos requeridos. También lo hice para forzarme a escribir más código y a seguir entendiendo qué es lo que estabamos intentando hacer.