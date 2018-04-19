package com.haiyisoft.bds.api.action

import com.haiyisoft.bds.api.data.Model
import org.apache.spark.sql.DataFrame

/** 模型训练（算法）实现，目的在于训练和保存模型
  * Created by XingxueDU on 2018/3/15.
  *
  * @version alpha build 3
  */
trait ModelTraining[M<: Model] extends Action[M]{
  /**
    * 模型训练
 *
    * @param data 训练集
    * @return 生成的模型
    */
  def fit(data: DataFrame): M
}
