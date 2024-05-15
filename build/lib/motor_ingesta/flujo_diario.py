import json
from datetime import timedelta
from loguru import logger
from motor_ingesta.motor_ingesta import MotorIngesta
from motor_ingesta.agregaciones import aniade_hora_utc, aniade_intervalos_por_aeropuerto

from pyspark.sql import SparkSession, functions as F


class FlujoDiario:

    def __init__(self, config_file: str):
        """
        Inicializa una instancia de FlujoDiario, cargando la configuración desde un archivo JSON y estableciendo la sesión de Spark.

        :param config_file: Ruta hacia el archivo de configuración en formato JSON que contiene las configuraciones necesarias para ejecutar los procesos diarios.
        :type config_file: str
        """
        # Leer como diccionario el fichero json indicado en la ruta config_file, usando json.load(f) del paquete json
        # y almacenarlo en self.config. Además, crear la SparkSession si no existiese usando
        # SparkSession.builder.getOrCreate() que devolverá la sesión existente, o creará una nueva si no existe ninguna

        self.spark = SparkSession.builder.getOrCreate()
        
        with open(config_file, 'r') as f:
            self.config = json.load(f)


    def procesa_diario(self, data_file: str):
        """
        Procesa los datos de vuelo diariamente a partir del archivo especificado, realizando la ingestión de datos, transformaciones y almacenamiento en la tabla de salida.

        :param data_file: Ruta hacia el archivo de datos que se va a procesar.
        :type data_file: str
        :return: None
        """

        # raise NotImplementedError("completa el código de esta función")   # borra esta línea cuando resuelvas
        try:
            # Procesamiento diario: crea un nuevo objeto motor de ingesta con self.config, invoca a ingesta_fichero,
            # después a las funciones que añaden columnas adicionales, y finalmente guarda el DF en la tabla indicada en
            # self.config["output_table"] y con los ficheros físicos en self.config["output_path"],
            # siempre particionando por FlightDate. Tendrás que usar .write.option("path", ...).saveAsTable(...) para
            # indicar que queremos crear una tabla externa en el momento de guardar.
            # Conviene cachear el DF flights_df así como utilizar el número de particiones indicado en
            # config["output_partitions"]

            motor_ingesta = MotorIngesta(self.config)
            flights_df = motor_ingesta.ingesta_fichero(data_file)
            
            # Paso 1. Invocamos al método para añadir la hora de salida UTC
            flights_with_utc = self.aniade_hora_utc(flights_df)


            # -----------------------------
            #  CÓDIGO PARA EL EJERCICIO 4
            # -----------------------------
            # Paso 2. Para resolver el ejercicio 4 que arregla el intervalo faltante entre días,
            # hay que leer de la tabla self.config["output_table"] la partición del día previo si existiera. Podemos
            # obviar este código hasta llegar al ejercicio 4 del notebook
            dia_actual = flights_df.first().FlightDate
            dia_previo = dia_actual - timedelta(days=1)
            try:
                flights_previo = self.spark.read.table(self.config["output_table"]).where(F.col("FlightDate") == dia_previo)
                logger.info(f"Leída partición del día {dia_previo} con éxito")
            except Exception as e:
                logger.info(f"No se han podido leer datos del día {dia_previo}: {str(e)}")
                flights_previo = None

            if flights_previo:
                # añadir columnas a F.lit(None) haciendo cast al tipo adecuado de cada una, y unirlo con flights_previo.
                # OJO: hacer select(flights_previo.columns) para tenerlas en el mismo orden antes de
                # la unión, ya que la columna de partición se había ido al final al escribir
                for field in ['FlightTime_next', 'Airline_next', 'diff_next']:
                                flights_with_utc = flights_with_utc.withColumn(field, F.lit(None).cast("string"))

                # Spark no permite escribir en la misma tabla de la que estamos leyendo. Por eso salvamos
                df_unido = flights_with_utc.unionByName(flights_previo)
                df_unido.write.mode("overwrite").saveAsTable("tabla_provisional")
                df_unido = self.spark.read.table("tabla_provisional")
            else:
                df_unido = flights_with_utc           # lo dejamos como está

            # Paso 3. Invocamos al método para añadir información del vuelo siguiente
            df_with_next_flight = self.aniade_intervalos_por_aeropuerto(df_unido)

            # Paso 4. Escribimos el DF en la tabla externa config["output_table"] con ubicación config["output_path"], con
            # el número de particiones indicado en config["output_partitions"]
            # df_with_next_flight.....(...)..write.mode("overwrite").option("partitionOverwriteMode", "dynamic")....

            df_with_next_flight.coalesce(self.config.get("output_partitions", 1))\
                               .write.mode("overwrite")\
                               .option("path", self.config["output_path"])\
                               .option("partitionOverwriteMode", "dynamic")\
                               .saveAsTable(self.config["output_table"])


            # Borrar la tabla provisional si la hubiéramos creado
            self.spark.sql("DROP TABLE IF EXISTS tabla_provisional")

        except Exception as e:
            logger.error(f"No se pudo escribir la tabla del fichero {data_file}")
            print(str(e))
            raise e
        
    def aniade_hora_utc(self, df):
        """
        Añade la hora UTC al DataFrame especificado.
        """
        return aniade_hora_utc(self.spark, df)
    
    def aniade_intervalos_por_aeropuerto(self, df):
        """
        Añade información de los vuelos siguientes
        """
        return aniade_intervalos_por_aeropuerto(df)
