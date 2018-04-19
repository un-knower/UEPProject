package example

import com.haiyisoft.bds.api.action.{Model, ModelTraining}
import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.sql.{Dataset, Row}

import scala.collection.JavaConversions._

/**
  * Created by XingxueDU on 2018/3/15.
  */
class LR extends ModelTraining[LRModel]{
  protected val lr = new LogisticRegression()
  private val default = Map(("featuresCol","features"),("labelCol","label"))

  /**
    * 读取保存路径
    */
  override def getSavePath: String = getParam("savePath").toString

  /**
    * DefaultParam中是否包含指定key
    *
    * @param key 带查询的参数名
    * @return true代表存在
    */
  override def containsDefaultParam(key: String): Boolean = default.contains(key)

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = default.map{key=>
    ParamFromUser.String(key._1).setDefaultValue(key._2)
  }.toArray

  /**
    * 获取默认参数
    *
    * @param key 参数名
    * @return 参数值
    */
  override def getDefaultParam(key: String): Any = default.getOrElse(key,null)

  override def fit(data: Dataset[Row]): LRModel = {
    val model: LogisticRegressionModel = lr.setFeaturesCol(getParam("featuresCol").toString).fit(data)
    new LRModel(model)
  }

  override def setSavePath(path: String): this.type = {
    addParam("savePath",path)
    this
  }

  override def run(data: Dataset[Row]): Unit = {
    assert(containsDefaultParam("savePath"))

    lr.setFeaturesCol(getParam("featuresCol").toString)
    lr.setLabelCol(getParam("labelCol").toString)
    val model = fit(data)
    save(model)
  }

  /**
    * 结果保存方法.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: LRModel): Unit = ???

  /**
    * 获取保存方式
    *
    * @return 保存方式
    */
override def getSaveMode: String = getParam("LR.save.mode").toString

  /**
    * 设置保存方式，如覆盖，追加等
    * overwrite
    *
    * @param mode 保存方式
    */
  override def setSaveMode(mode: String): LR.this.type = addParam("LR.save.mode",mode)
}
