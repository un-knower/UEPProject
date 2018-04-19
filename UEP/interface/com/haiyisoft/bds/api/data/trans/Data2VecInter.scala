package com.haiyisoft.bds.api.data.trans

import com.haiyisoft.bds.api.data.DataTransformInter
import com.haiyisoft.bds.api.param.col.{HasInputParam, HasOutputParam}
import org.apache.spark.ml.linalg.Vector


/**
  * 将复数列数据组合成[[Vector]]的方法。vector用于大部分模型训练
  * Created by XingxueDU on 2018/3/15.
  *
  * @version 0.1.0 build 2
  */
trait Data2VecInter extends DataTransformInter with HasOutputParam with HasInputParam{


}