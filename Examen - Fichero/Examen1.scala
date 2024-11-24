package job.examen


import org.apache.spark.rdd.RDD
import org.apache.spark.sql.functions._
import org.apache.spark.sql.ColumnName

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.{DataFrame, SparkSession, functions}
import org.apache.spark.sql.functions._

object Examen1 {

  /**Ejercicio 1: Crear un DataFrame y realizar operaciones básicas
   Pregunta: Crea un DataFrame a partir de una secuencia de tuplas que contenga información sobre
   estudiantes (nombre, edad, calificación).
   Realiza las siguientes operaciones:

   Muestra el esquema del DataFrame.
   Filtra los estudiantes con una calificación mayor a 8.
   Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente.
   */
  def ejercicio1(estudiantes: DataFrame)(spark:SparkSession): DataFrame = {
    estudiantes.createOrReplaceTempView("tablaEstudiantes")
    println("**** Ejercicio 1 ****")

    println("__ Muestra el esquema del DataFrame __")
    estudiantes.printSchema()

    println("Filtra los estudiantes con una calificación mayor a 8")
    estudiantes.filter(col("Calificacion") > 8.0).show()

    println("Selecciona los nombres de los estudiantes y ordénalos por calificación de forma descendente")
    estudiantes.select("Nombre").orderBy(desc("Calificacion")).show()

    estudiantes


  }

  /**Ejercicio 2: UDF (User Defined Function)
   Pregunta: Define una función que determine si un número es par o impar.
   Aplica esta función a una columna de un DataFrame que contenga una lista de números.
   */
  def ejercicio2(numeros: DataFrame)(spark:SparkSession): DataFrame =  {
    println("**** Ejercicio 2 ****")


    def esPar(numero: Int): Boolean = numero % 2 == 0
    val esParUDF = udf(esPar _)

    numeros.withColumn("Par" , esParUDF(col("Numero"))).show()

    numeros
  }

  /**Ejercicio 3: Joins y agregaciones
   Pregunta: Dado dos DataFrames,
   uno con información de estudiantes (id, nombre)
   y otro con calificaciones (id_estudiante, asignatura, calificacion),
   realiza un join entre ellos y calcula el promedio de calificaciones por estudiante.
   */
  def ejercicio3(estudiantes: DataFrame , calificaciones: DataFrame): DataFrame = {
    println("**** Ejercicio 3 ****")
    val estudiantesCalificaciones = estudiantes
      .join(calificaciones, estudiantes("id") === calificaciones("id_Estudiante"))
      .select("id", "nombre", "asignatura", "calificacion")

    val resultado = estudiantesCalificaciones.groupBy("id").agg(avg("calificacion").alias("Promedio"))
    resultado.show()
    resultado
  }

  /**Ejercicio 4: Uso de RDDs
   Pregunta: Crea un RDD a partir de una lista de palabras y cuenta la cantidad de ocurrencias de cada palabra.

   */

  def ejercicio4(palabras: List[String])(spark:SparkSession): RDD[(String, Int)] = {
    val rddPalabras = spark.sparkContext.parallelize(palabras)
    val totalPalabras = rddPalabras
      .map((_, 1))  //
      .reduceByKey(_ + _)

    println("**** Ejercicio 4 ****")
    totalPalabras.collect().foreach(println)
    totalPalabras
  }





  /**
   Ejercicio 5: Procesamiento de archivos
   Pregunta: Carga un archivo CSV que contenga información sobre
   ventas (id_venta, id_producto, cantidad, precio_unitario)
   y calcula el ingreso total (cantidad * precio_unitario) por producto.
   */
  def ejercicio5(ventas: DataFrame)(spark:SparkSession): DataFrame = {

    val agrupacionProductos = ventas.groupBy("id_producto").agg(sum("cantidad").alias("TotalCantiad"), sum("precio_unitario").alias("TotalPrecioUnitario"))
    val ingresoTotal = agrupacionProductos.withColumn("Total", col("TotalCantiad") * col("TotalPrecioUnitario") )
    println("**** Ejercicio 5 ****")
    ingresoTotal.show()
    ingresoTotal
  }

}
