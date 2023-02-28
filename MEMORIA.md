# MEMORIA TRABAJO SQL

## Realizada por Miguel Ramón Alonso para el módulo de SQL del Máster de Bioinformática

###### Organizado por el ISCIII en colaboración con el BSC y otras instituciones

### Primera entrada: 18.35 20/01/2023

**super preliminar**

El primer paso fue analizar cual era el problema a resolver. En el primer ejercicio, la intención es realizar una descarga de datos provenientes de la base de datos de Clinvar, utilizando para ello versiones congeladas del 06/2022 y 12/2022.
Tras revisar la información contenida en el archivo README de manejo de la base de datos de Clinvar, se observó que para la realización del primer apartado de este primer ejercicio (informe sobre bibliografía existente de la variante indicada), la información se hallaba presente en el fichero *var_citations.txt*. 
Entiendo que lo que tengo que hacer es relacionar esta información con un fichero programa principal (parecido al parseador de ejemplo). Es verdad que me confunde bastante esta parte: 

> Todos los programas deberán crear sus propias tablas sobre la base de datos que se le indique en la entrada, pero deberán ser capaces de trabajar sobre bases de datos ya existentes. Concretamente, sobre las bases de datos generadas al cargar una release en concreto de ClinVar usando clinvar_parser.py, para poder lanzar consultas combinadas.
> El objetivo es lanzar dos tandas de los programas, para generar dos bases de datos diferentes, una de datos de junio de 2022 y otra de diciembre de 2022. Esas bases de datos son necesarias para contestar a las preguntas de la segunda parte.

No entiendo del todo a que se refiere, no se si precisa de dos scripts diferentes para cada una de las versiones con las que estamos trabajando (incluyendo un script principal?). En cualquier caso tengo que volver a mirar como está distribuída la base de datos.

Para el segundo apartado (analisis estadistico de variantes por gen) el fichero a utilizar es *gene_specific_summary.txt*. Me ofrece las mismas dudas que las anteriores.

El script de CIViC en cambio parece más sencillo de entender, ya que no debería ser demasiado diferente del de clinvar. Cambia la forma de distribuirse el fichero (*txt*/t a *tsv*?, no demasiado diferente o no debería) y evidentemente cambian los nombres y distribución de las columnas; más allá de eso, no debería ser muy diferente.