# MEMORIA TRABAJO SQL

## Realizada por Miguel Ramón Alonso para el módulo de SQL del Máster de Bioinformática

###### Organizado por el ISCIII en colaboración con el BSC y otras instituciones

### INTRODUCCIÓN

El objetivo de este trabajo consistió en aprender a trabajar con bases de datos relacionales SQL, utilizando la libreria OSS SQLite. Para la creación y modificación de dichas bases de datos, se propuso utilizar diversos scripts escritos en Python, los cuales se encargarían de relacionar la información contenida en los diversos ficheros almacenados en los históricos de ClinVar y CIViC requeridos para la realización de la práctica. Específicamente, se requirió el procesado de la base de datos de variantes de ClinVar, en sus versiones congeladas de junio y diciembre de 2022, mediante el script *clinvar_parser.py*; a continuación, debíamos escribir dos scripts más, basados en éste, los cuales se encargaran de añadir a la BBDD creada por dicho script la información hallada en los ficheros de citaciones y de estadísticas específicas de cada gen.  Paralelamente, debíamos realizar un script similar al modelo utilizado en clase para el parseo de la base de datos de variantes contenida en CIViC. 

### PRIMERA PARTE

#### 1) Carga de fichero de referencias de ClinVar en la BBDD generada por *clinvar_parser.py*

Misma lógica programática seguida en el script *clinvar_parser.py*, creando una nueva tabla específica para la carga de las citaciones contenidas en el fichero *var_citations.txt*, y simplificando el código.



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