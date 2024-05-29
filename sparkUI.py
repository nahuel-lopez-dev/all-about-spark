# Instalar SDK java 8

!apt-get install openjdk-8-jdk-headless -qq > /dev/null

# Descargar Spark

!wget -q https://archive.apache.org/dist/spark/spark-3.3.4/spark-3.3.4-bin-hadoop3.tgz

# Descomprimir la version de Spark

!tar xf spark-3.3.4-bin-hadoop3.tgz

# Establecer las variables de entorno

import os

os.environ["JAVA_HOME"] = "/usr/lib/jvm/java-8-openjdk-amd64"
os.environ["SPARK_HOME"] = "/content/spark-3.3.4-bin-hadoop3"

# Descargar findspark

!pip install -q findspark

# Crear la sesión de Spark

import findspark

findspark.init()

from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .config('spark.ui.port', '4050')
    .getOrCreate()
)

from google.colab import output

output.serve_kernel_port_as_window(4050, path='/jobs/index.html')

from pyspark.sql.functions import col

spark.range(10000).toDF("id").filter(col('id') / 2 == 0).write.mode('overwrite').parquet('/output')