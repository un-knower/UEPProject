package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.Data2VecInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.HasMultipleInputParamByName
import org.apache.spark.ml.feature.{VectorAssembler => MLTransformer}
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by xujing on 2018/3/19.
  */
class VectorAssembler extends Data2VecInter with HasMultipleInputParamByName {

  override def transform(data: Dataset[Row]): Dataset[Row] = {
    val outCol = getOutputCol
    val inputCols = getInputColsImpl
    val std = new MLTransformer()
    std.setInputCols(inputCols)
    std.setOutputCol(outCol)
    std.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = multipleInputColFromUser("Double",SYMBOL_SEPARATOR)
    val outputCol = outputColFromUser("ml.Vector").addDefaultValue("features")
    Array(inputCol,outputCol)
  }

  override def getPluginName: String = "特征向量生成器"
}
