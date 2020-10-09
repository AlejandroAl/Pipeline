# Pipeline
Este repository contiene un ejemplo de ETL utilizando diferentes origenes de datos y diferentes salidas.


# Develop

El desarrollo esta orquestado por Airflow, el cual nos permite el desarrollo e implementación de forma agil, permite agregar multiples operadores o sensores, expresiones de tiempo y execución de procesos en parelelo.

## Carga de información

Se eligio utilizar Mysql como carga de datos, en este ejemplo al contar con mucha información y la consulta de los datos no involucra multiples peticiones, se adapta bien Mysql.

## Extracción 

Para la extración de datos, al tener un cumulo de información no grande, se utilizo el lenguaje de progración python, el cual se adapta bien en la incoorporación con airflow, python nos permite desarrollo ETLS de manera sencilla con su modulos, como lo es pandas, numpy, etc.

El formato que se utilizo para guarda la información fue parquet, se evaluo entre parquet y css, el ganador fue parquet debido a su forma de guardar los datos (binario) y nos permite conservar el schema de datos a diferencía de los archivos csv.

## Transformación 

Se utilizó python para aplicar las transformaciones, unos de los primeros retos, para convertir los datos de fecha timestamp, fue el formato que se tenian las columnas, por lo cual se diseño una pequeña función para convertir el str a timestamp, en caso de no cumplir con el formato se asigna una fecha por default (1700-01-01), este valor dependerá mucho de las reglas de negocio.

## Dispersión de la información 

En este paso se sigue trabajando con python por su versatilidad con el manejo de los datos a través de su libreria y modulos pandas. 

Uno de los retos fue limpiar los datos correspondientes a las compañias, exisian datos repetidos, identificadores vacios (None, Nan) y en una ocación se visualizón un identificador con formato "*****". Para el tratamiento de los datos se generarón dos funciones para sustituir los valores nulos y despues desechar los valores con no daban valor (aqui se puede aplicar una regla de negocio antes de eliminar los registros), en los nombres de las compañias existia una similitud y fueron organizados en una solo.

El proceso genera dos tablas e inserta la información correspondientes. A continuación se muestra el esquema de datos:


 ![RelationalDiagraman](https://github.com/AlejandroAl/Pipeline/blob/main/DiagramaRelacional.jpg)

[click link here to see image](https://github.com/AlejandroAl/Pipeline/blob/main/DiagramaRelacional.jpg)

# Deploy

Requerimientos:

Encontrarse en un entorno linux.

Instalación de docker, agregar su usario al grupo de docker para le ejecución de los comandos.

[link instalación docker](https://docs.docker.com/engine/install/)

Instalación de docker-compose 

[link instalación docker-compose](https://docs.docker.com/compose/install/)

Tener inslalado git.

Pasos para la validación desplegar los servicios:

1. Realizar el git clone del repository

```
git clone https://github.com/AlejandroAl/Pipeline.git
```

2. Ubicarse en la carpeta raiz del proyecto 

```
cd {ubicación descarga}/Pipeline
```

3. Ejecutar el script setup.sh

```
. script/setup.sh 
```

4. Ejecutar el comando 

```
docker-compose up -d --build

```

5. Los servicios tardan un poco el levantar, esperando a mysql (este toma al rededor de 3 min máximo). Para monitorear el servicio ejecutar el siguiente comando:

```
docker logs -f pipeline_webserver_1
```

El comando anterior mostrará el log del servicio que se encuentra levantandose, podria apaecer valors mensajes como los siguientes:


waiting for mysql 4/100
waiting for postgres 4/100

Los cuales indican que aun faltan por levantar los servicios de las base de datos.

Cuando se muestre en la terminal el logo de airflow, el servicio a finalizado de levantar.

Una vez que se visualice este contenido, esposible navegar a través de un explorador web la consola de airflow.

http://localhost:8081

En la consola se podrá observar el pipeline el cual solo se ejecutará una vez, con base en la configuración que se realizó, este puede cambiar si se requieré.


6. Para validar si nuestro pipeline a terminado dar clik en el servicio llamado "pipeline_conekta".

Este mostrara los pasos, los pasos deberán estar en color verde oscuro, lo que indica que nuestro servicio a finalizado.


Para ver los datos resultado, podemos utilizar un cliente de base dedatos, como DataGrip o dbvisualizer, e ingresar los siguientes datos

USER: airflow
password: airflow
database: conekta
host: localhost
port: 5432

Una vez establecida la conección podremos visualizar las dos tablas creadas por el pipeline