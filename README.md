# spark-tarea-final
Repositorio para la tarea final del curso de Spark

## Instrucciones:
* Clonar este repositorio desde un IDE (recomendado: PyCharm o Visual Studio)
* Crear un entorno virtual en la carpeta del proyecto, con venv en PyCharm o similar. Necesario python >= 3.10.
  * En PyCharm: File -> Settings... -> Project -> Python interpreter -> Add interpreter -> Add local interpreter. No
  marcar la casilla de "Inherit global site-packages".
  * Abrir una terminal de Windows (o el SO que estemos usando) y activar el entorno virtual creado.
  * Instalar el contenido del fichero requirements con `pip install -r requirements.txt` que incluye pyspark==3.5.0
  * Para que funcione adecuadamente Spark en Windows, es necesario haber seguido las instrucciones del mensaje de 
  bienvenida del foro, donde se explica cómo instalar la JDK, configurar la variable de entorno JAVA_HOME y 
  también HADOOP_HOME tras descargar winutils y la biblioteca DLL de Hadoop para Windows.

Una vez clonado, las instrucciones se encuentran en [este notebook](notebooks/actividad_spark.ipynb). La idea es
generar un paquete de Python, instalarlo y probar su funcionamiento en el notebook. Esto se puede hacer en el propio
portátil del alumno, ya que los ficheros de datos que estamos probando son pequeños, o bien se puede subir el
paquete (fichero wheel) al cluster de Databricks en el apartado Libraries, subir también el notebook al Workspace,
y subir los ficheros de datos a una ubicación de Azure Data Lake Storage v2 para leerlos desde el notebook.

Si optas por esto último, recuerda revisar la guía de creación de clusters en Databricks adjunta al mensaje de
bienvenida.
