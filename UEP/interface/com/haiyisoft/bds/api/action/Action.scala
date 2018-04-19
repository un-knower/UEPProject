package com.haiyisoft.bds.api.action

import com.haiyisoft.bds.api.data
import com.haiyisoft.bds.api.param.WithParam
import com.haiyisoft.bds.api.param.save.HasSaveParamFor
import org.apache.spark.sql.DataFrame

/** 流程的最终执行部分，关键需要实现run方法
  * action的含义是：输入DataFrame但是无输出
  *
  * 如果一个方法既可以作为流程又可以作为输出，则应该同时实现[[Action]]和[[data.DataTransformInter]]两个特性
  *
  * Created by XingxueDU on 2018/3/15.
  *
  * @tparam T 需要被保存的数据类型
  * @version alpha build 3
  */
trait Action[T] extends WithParam with HasSaveParamFor[T]{

  /**
    * 执行算法,应该被覆盖
    */
  def run(data: DataFrame):Unit={
    this match {
      case x:ModelTraining[T] =>
        val model = x.fit(data)
        x.save(model)
      case d:DataFrameAction =>
        d.save(data)
      case _ =>
        throw new NoSuchMethodError(s"need override for method $this.run")
    }
  }
}
