package com.haiyisoft.bds.api.data

import com.haiyisoft.bds.api.param.HasLoadParam

/** 模型
  * Created by XingxueDU on 2018/3/15.
  *
  * @version alpha build 3
  */
trait Model extends DataTransformInter with HasLoadParam{

  /**
    * 模型的读取方法
    * 这个方法用于将model本身加载为保存在path位置的模型
    * 这个方法应该在transform中被自身调用
 *
    * @param path 读取地址
    */
  protected def loadFrom(path: String): this.type


}
