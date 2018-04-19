package com.haiyisoft.bds.api.data.trans

import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasMultipleInputParamByColumn, HasOutputParam}
import org.apache.spark.sql._
import org.apache.spark.sql.expressions.UserDefinedFunction

/**
  * Created by XingxueDU on 2018/3/15.
  *
  *
  */
trait Udf2DataMining extends DataProcessInter with HasOutputParam with HasMultipleInputParamByColumn{
  protected var myudf:UserDefinedFunction = null
  protected def udfInputColNum:Int
  var defaultParamList:Map[String,String]=Map[String,String]()
  def setDefaultParamList(map:Map[String,String])={
    defaultParamList++=map
  }
  def initialize():this.type
  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val input = multipleInputColFromUser()
      .setDescription("请输入传入列，应输入"+udfInputColNum+"列")
    val output = outputColFromUser()
    val df = defaultParamList.map{key=>
      ParamFromUser.String(key._1).addDefaultValue(key._2)
    }.toArray
    input+:output+:df
  }


  override def transform(data: Dataset[Row]): Dataset[Row] = {
    initialize()
    val columns = getInputCols
    val outCol = getOutputCol
    data.withColumn(outCol,myudf(columns:_*))
  }

  override def toString = this.getClass+"[Udf2DataMining]:"+myudf

  override def getPluginName: String = "UDF转换器"
}
object Udf2DataMining{
  def apply(udf:UserDefinedFunction,output:String,input:String,othersInput:String*):Udf2DataMining = {
    val length = udf.inputTypes.get.size
    val res = new Udf2DataMining(){

      override def initialize(): this.type = {
        this.myudf = udf
        this
      }

      override protected def udfInputColNum: Int = length
    }
    res.setOutputCol(output)
    res.setInputCols(input,othersInput:_*)
    res
  }
}