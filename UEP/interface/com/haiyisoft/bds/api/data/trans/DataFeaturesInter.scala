package com.haiyisoft.bds.api.data.trans

import com.haiyisoft.bds.api.data.DataTransformInter
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasOutputParam}

/**
  * 变换方法，对features进行变换
  * Created by XingxueDU on 2018/3/15.
  */
trait DataFeaturesInter extends DataTransformInter with HasFeaturesColParam with HasOutputParam {

}
