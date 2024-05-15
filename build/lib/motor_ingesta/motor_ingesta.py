
import json
from pyspark.sql import DataFrame as DF, functions as F, SparkSession


class MotorIngesta:
    """
    Esta clase proporciona funcionalidades para la ingestión de datos en formato JSON,
    procesamiento y aplanamiento de DataFrames dentro de una sesión de Apache Spark.
    """
    def __init__(self, config: dict):
        """
        Inicializa una instancia de MotorIngesta configurando una sesión de Spark y 
        almacenando configuraciones para la ingestión de datos.

        :param config: Un diccionario que contiene configuraciones necesarias para la ingestión
                       y procesamiento de los datos, incluyendo detalles sobre las columnas.
        :type config: dict
        """
        self.config = config
        self.spark = SparkSession.builder.getOrCreate()

    def ingesta_fichero(self, json_path: str) -> DF:
        """
        Lee un archivo JSON y lo convierte en un DataFrame de Spark. El DataFrame resultante
        se aplanará según la configuración especificada y se seleccionarán las columnas requeridas,
        aplicando el tipo de datos y metadatos especificados.

        :param json_path: Ruta del archivo JSON que se va a procesar.
        :type json_path: str
        :return: Un DataFrame de Spark que ha sido aplanado y configurado según lo especificado en `config`.
        :rtype: pyspark.sql.DataFrame
        """
        # Leemos el JSON como DF, tratando de inferir el esquema, y luego lo aplanamos.
        # Por último nos quedamos con las columnas indicadas en el fichero de configuración,
        # en la propiedad self.config["data_columns"], que es una lista de diccionarios. Debemos recorrer
        # esa lista, seleccionando la columna y convirtiendo cada columna al tipo indicado en el fichero.

        # PISTA: crear en lista_obj_column una lista de objetos Column como lista por comprensión a partir
        # de self.config["data_columns"], y luego usar dicha lista como argumento de select(...). El DF resultante
        # debe ser devuelto como resultado de la función.

        # Para incluir también el campo "comment" como metadatos de la columna, podemos hacer:
        # F.col(...).cast(...).alias(..., metadata={"comment": ...})

        flights_day_df = self.spark.read.json(json_path)

        # Flatten the DataFrame
        aplanado_df = self.aplana_df(flights_day_df)
        
        lista_obj_column = [
            F.col(diccionario['name']).cast(diccionario['type']).alias(
                diccionario['name'], metadata={"comment": diccionario.get('comment', '')}
            ) for diccionario in self.config["data_columns"]
        ]
        
        resultado_df = aplanado_df.select(*lista_obj_column)
        return resultado_df

    @staticmethod
    def aplana_df(df: DF) -> DF:
        """
        Aplana un DataFrame de Spark que tenga columnas de tipo array y de tipo estructura.

        :param df: DataFrame de Spark que contiene columnas de tipo array o columnas de tipo estructura, incluyendo
                   cualquier nivel de anidamiento y también arrays de estructuras. Asumimos que los nombres de los
                   campos anidados son todos distintos entre sí, y no van a coincidir cuando sean aplanados.
        :return: DataFrame de Spark donde todas las columnas de tipo array han sido explotadas y las estructuras
                 han sido aplanadas recursivamente.
        """
        to_select = []
        schema = df.schema.jsonValue()
        fields = schema["fields"]
        recurse = False

        for f in fields:
            if f["type"].__class__.__name__ != "dict":
                to_select.append(f["name"])
            else:
                if f["type"]["type"] == "array":
                    to_select.append(F.explode(f["name"]).alias(f["name"]))
                    recurse = True
                elif f["type"]["type"] == "struct":
                    to_select.append(f"{f['name']}.*")
                    recurse = True

        new_df = df.select(*to_select)
        return MotorIngesta.aplana_df(new_df) if recurse else new_df
