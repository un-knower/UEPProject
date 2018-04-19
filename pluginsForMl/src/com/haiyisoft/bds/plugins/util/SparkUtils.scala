package com.haiyisoft.bds.plugins.util

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.ml.linalg.{Vector => NewVector,Vectors => NewVectors}
import org.apache.spark.mllib.linalg.{Vector => OldVector,Vectors => OldVectors}

import org.apache.spark.sql.functions._

import scala.reflect.ClassTag

/**
  * Created by Namhwik on 2018/3/17.
  */
private[bds] object SparkUtils {
  /**
    * 获取数值类型列的数据
    *
    * @param row        Row
    * @param columnName 列名
    * @param columnType 数据的SchemaMap
    * @return
    */
  def getRowValue(row: Row, columnName: String, columnType: String): Either[Double, String] = {
    columnType match {
      case "integer" => Left(row.getAs[Int](columnName).toDouble)
      case "double" => Left(row.getAs[Double](columnName))
      case "long" => Left(row.getAs[Long](columnName).toDouble)
      case "decimal" => Left(row.getAs[java.math.BigDecimal](columnName).doubleValue())
      case "string" => Left(row.getAs[String](columnName).toDouble)
      case _ => Right(row.getAs[String](columnName))
    }
  }

  /**
    * 获取数据的SchemaMap
    *
    * @param dataframe DataFrame
    * @return
    */
  def getColumnType(dataframe: DataFrame): Map[String, String] = {
    dataframe.schema.map(x => {
      val typeName = if (x.dataType.typeName.startsWith("decimal")) "decimal" else x.dataType.typeName
      (x.name, typeName)
    }).toMap
  }

  /**
    * 数组相加
    *
    * @param arr0 Array[T]
    * @param arr1 Array[T]
    * @tparam T ClassTag
    * @return
    */
  def reducer[T: ClassTag](arr0: Array[T], arr1: Array[T], operationsTypes: Array[String]): Array[Double] = {
    if (arr0.length == arr1.length && arr0.length == operationsTypes.length) {
      operationsTypes.indices.map(
        index => {
          val a = arr0(index).toString.toDouble
          val b = arr1(index).toString.toDouble
          operationsTypes(index).toUpperCase match {
            case "COUNT" => a + b
            case "SUM" => a + b
            case "MAX" => if (a > b) a else b
            case "MIN" => if (a > b) b else a
          }
        }
      ).toArray
    }
    else
      throw new RuntimeException("reducer arr length can not match ...")
  }

  /**
    * 获取分类类型列的数据
    *
    * @param row           Row
    * @param columnName    列名
    * @param columnTypeMap 数据的SchemaMap
    * @return
    */
  def getDiscretedRowValue(row: Row, columnName: String, columnTypeMap: Map[String, String]): String = {
    columnTypeMap(columnName) match {
      case "string" => row.getAs[String](columnName)
      case "integer" => row.getAs[Int](columnName).toString
      case "double" => row.getAs[Double](columnName).toString
      case "long" => row.getAs[Long](columnName).toDouble.toString
      case "decimal" => row.getAs[java.math.BigDecimal](columnName).toString
    }
  }
  /**
    * 数组相加
    *
    * @param arr0
    * @param arr1
    * @tparam T
    * @return
    */
  def reducer[T: ClassTag](arr0: Array[T], arr1: Array[T]): Array[Double] = {
    arr0.indices.map(index => arr0(index).toString.toDouble + arr1(index).toString.toDouble).toArray
  }


  /**
    * 二维数组相加
    *
    * @param arr0
    * @param arr1
    * @tparam T
    * @return
    */
  def reducer2[T: ClassTag](arr0: Array[Array[T]], arr1: Array[Array[T]]): Array[Array[Double]] = {
    arr0.indices.map(index => reducer(arr0(index), arr1(index))).toArray
  }

  /**
    * 可变长udf
    *
    * @param function in集合（所有列值）总结计算为out的方法
    * @param num_cols 期望的udf长度
    */
  def LengthChangeableUdf[IN,OUT](function:Seq[IN]=>OUT)(num_cols:Int)(implicit str:scala.reflect.runtime.universe.TypeTag[IN],cls:scala.reflect.ClassTag[IN], input:scala.reflect.runtime.universe.TypeTag[OUT])={
    import org.apache.spark.sql.functions._
    num_cols match{
      case 1 =>udf{(atts:IN) => function(Array(atts))}
      case 2 =>udf{(a:IN,b:IN) => function(Array(a,b))}
      case 3 =>udf{(a:IN,b:IN,c:IN) => function(Array(a,b,c))}
      case 4 =>udf{(a:IN,b:IN,c:IN,d:IN) => function(Array(a,b,c,d))}
      case 5 =>udf{(a:IN,b:IN,c:IN,d:IN,e:IN) => function(Array(a,b,c,d,e))}
      case 6 =>udf{(a:IN,b:IN,c:IN,d:IN,e:IN,f:IN) => function(Array(a,b,c,d,e,f))}
      case 7 =>udf{(a:IN,b:IN,c:IN,d:IN,e:IN,f:IN,g:IN) => function(Array(a,b,c,d,e,f,g))}
      case 8 =>udf{(a:IN,b:IN,c:IN,d:IN,e:IN,f:IN,g:IN,h:IN) => function(Array(a,b,c,d,e,f,g,h))}
      case 9 =>udf{(a:IN,b:IN,c:IN,d:IN,e:IN,f:IN,g:IN,h:IN,i:IN) => function(Array(a,b,c,d,e,f,g,h,i))}
      case 10 =>udf{(a1:IN,b1:IN,c1:IN,d1:IN,e1:IN,f1:IN,g1:IN,h1:IN,i1:IN,j1:IN) => function(Array(a1,b1,c1,d1,e1,f1,g1,h1,i1,j1))}
      case _ => throw new NoSuchMethodException(num_cols+" > 10")
    }
  }
  def AddColsWithLengthChangeableUDF[IN,OUT](UdfFunc:Seq[IN]=>OUT, df:DataFrame,outputColName:String, inputColNames:String*)(implicit str:scala.reflect.runtime.universe.TypeTag[IN],cls:scala.reflect.ClassTag[IN], input:scala.reflect.runtime.universe.TypeTag[OUT]): DataFrame ={
    val AnalyseColsSplit = SeqUtils.splitArrayByLength(inputColNames,10)
    println("Cols To Analyse:")
    println(AnalyseColsSplit.mkString("\n"))
    if(AnalyseColsSplit.length==1) {
      val udf = LengthChangeableUdf(UdfFunc)(inputColNames.length)
      df.withColumn(outputColName,udf((for(x<-inputColNames)yield col(x)):_*))
    }
    else {
      var tmpdf = df
      for(x<-AnalyseColsSplit.indices){
        val in = tmpdf
        val udf = LengthChangeableUdf[IN,Seq[IN]](k=>k)(AnalyseColsSplit(x).length)
        val res = in.withColumn(outputColName+"#"+x,udf((for(x<-AnalyseColsSplit(x))yield col(x)):_*))
        tmpdf = res
      }
      val df3 = unionVectors(tmpdf, outputColName, outputColName + "#0", outputColName + "#1", (for (x <- 2 until AnalyseColsSplit.length) yield outputColName + "#" + x): _*)
      val df4 = df3.drop((for (x <- AnalyseColsSplit.indices) yield outputColName + "#" + x): _*)
      val myudf = udf{(in:Seq[IN])=>
        UdfFunc(in)
      }
      df4.withColumn(outputColName,myudf(col(outputColName)))
    }
  }
  /**
    * 结合2个或2个以上vector列并组合成一个新列，用于结合由不同方法生成的vector
    *
    * @param input 用于转换的表
    * @param newColName 将要生成的列的名字
    * @param joinedCols 用于结合的列名，列不能少于2个
    */
  def unionVectors(input:DataFrame, newColName:String, joinedCol1:String, joinedCol2:String, joinedCols:String*):DataFrame = {
    var idx = 1

      val join = udf { (a: Object, b: Object) =>
        a match{
          case nv:NewVector => NewVectors.dense(nv.toArray ++ b.asInstanceOf[NewVector].toArray).compressed
          case ov:OldVector=> OldVectors.dense(ov.toArray ++ b.asInstanceOf[OldVector].toArray).compressed
          case ar:Array[_]=> ar++ b.asInstanceOf[Array[_]]
          case sq:Seq[_]=>sq++ b.asInstanceOf[Seq[_]]
          case _ => throw new ClassCastException(a.getClass.getName+" can't cast to a array")
        }

      }
      var data: DataFrame = input.withColumn("tmp" + idx, join(col(joinedCol1), col(joinedCol2)))
      for (name <- joinedCols) {
        val s = data
        data = s.withColumn("tmp" + ((idx + 1) % 2), join(col("tmp" + (idx % 2)), col(name))).drop(col("tmp" + (idx % 2)))
        idx += 1
      }
      data.withColumnRenamed("tmp"+(idx%2),newColName)

  }

}
