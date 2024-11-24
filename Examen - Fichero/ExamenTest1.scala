package Examen


import job.examen.Examen1.{ejercicio1, ejercicio2, ejercicio3, ejercicio4, ejercicio5}
import org.apache.spark.sql.types.{DoubleType, IntegerType, StringType, StructField, StructType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.functions.{col, lit}
import utils.TestInit


class ExamenTest1 extends TestInit {

  "ejercicio1" should "Crear un DataFrame y realizar operaciones básicas" in {
    import spark.implicits._
    val estudiantes = Seq(
      ("Ana", 20, 9.5),
      ("Juan", 22, 7.8),
      ("Luis", 19, 8.2),
      ("Marta", 21, 6.9),
      ("Pedro", 23, 9.0),
      ("Juan", 23, 6.0)
    ).toDF("Nombre","Edad","Calificacion")
    ejercicio1(estudiantes)(spark)
  }



  "ejercicio2" should "UDF (User Defined Function)" in {
    import spark.implicits._
    val numeros = Seq(1, 2, 3, 4, 5, 6, 7, 8, 9, 10).toDF("Numero")
    ejercicio2(numeros)(spark)
  }



  "ejercicio3" should "Joins y agregaciones" in {
    import spark.implicits._

    val estudiantes = Seq(
      (1, "Ana"),
      (2, "Luis"),
      (3, "María")
    ).toDF("id", "nombre")



    val calificaciones = Seq(
      (1, "Matemáticas", 8.5),
      (1, "Ciencias", 9.0),
      (2, "Matemáticas", 7.0),
      (2, "Ciencias", 6.5),
      (3, "Matemáticas", 8.0),
      (3, "Ciencias", 7.5)
    ).toDF("id_estudiante", "asignatura", "calificacion")

    ejercicio3(estudiantes,calificaciones)
  }


  "ejercicio4" should "Uso de RDDs" in {
    val rddPaises = List(
      "Siria", "Brasil", "Chile", "Colombia", "México", "España", "Honduras", "Siria", "Honduras", "Honduras",
      "Honduras", "Honduras", "Perú", "Honduras", "Honduras", "Barbados", "Puerto Rico", "Barbados", "Honduras", "Costa Rica",
      "Uruguay", "Paraguay", "Perú", "Honduras", "El Salvador", "México", "México", "México", "Siria", "Honduras",
      "Honduras", "Honduras", "Siria", "Perú", "México", "Siria", "Honduras", "Siria")

    ejercicio4(rddPaises)(spark)
   // val collectRDD = ejercicio4(rddPaises)(spark)



    //println("primera posición")
    //collectRDD.first() shouldBe("Francia",1)

  }



  "ejercicio5" should "Procesamiento de archivos" in {
    val ventasDF : DataFrame = spark.read.option("header", true)
      .csv("/Users/sandyrodriguezaponte/IdeaProjects/SR_Examen/src/test/resources/ventas.csv")
    ejercicio5(ventasDF)(spark)
  }







}
