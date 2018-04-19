package com.haiyisoft.bds.plugins.process.ml.feature

import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasOneInputParamByName, HasOutputParam}
import org.apache.spark.ml.feature.{IndexToString => MLTransformer}
import org.apache.spark.sql.{Dataset, Row}

/**
  * Created by xujing on 2018/3/19.
  *
  * 将通过[[StringIndex]]转化为的数值型label还原回String
  *
  * @see [[org.apache.spark.ml.feature.IndexToString]]
  * @author Xu Jing
  * @author DU Xingxue
  * @author Zhai Te
  */
class IndexToString extends DataProcessInter with HasOneInputParamByName with HasOutputParam{


  override def transform(data: Dataset[Row]): Dataset[Row] = {
    val outCol = getOutputCol
    val inputCol = getInputColsImpl
    val std = new MLTransformer()
    std.setInputCol(inputCol)
    std.setOutputCol(outCol)
    std.transform(data)
  }

  override def getNecessaryParamList: Array[ParamFromUser] = {
    val inputCol = oneInputColFromUser("Double")
    val outputCol = outputColFromUser("Double")
    Array(inputCol,outputCol)
  }

  override def getPluginName: String = "数字标签还原"
}
