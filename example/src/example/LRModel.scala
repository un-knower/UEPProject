package example

import com.haiyisoft.bds.api.action.Model
import com.haiyisoft.bds.api.param.{ParamFromUser, WithParam}
import org.apache.spark.ml.classification.LogisticRegressionModel
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConversions._

/**
  * Created by XingxueDU on 2018/3/15.
  */
class LRModel(private var model:LogisticRegressionModel = null) extends Model{
  private val default = Map("featuresCol"->"features")


  /**
    * 获取保存方式
    *
    * @return 保存方式
    */
override def getSaveMode: String = getStringParam("LRModel.save.mode")

  /**
    * 设置保存方式，如覆盖，追加等
    * overwrite
    *
    * @param mode 保存方式
    */
  override def setSaveMode(mode: String): LRModel.this.type = addParam("LRModel.save.mode",mode)
/**
    * 返回读取模型的地址，从该地址加载模型
    *
    * @return 模型保存的地址
    */
  override def getLoadPath: String = getStringParam("loadPath")



  /**
    * 读取保存路径
    */
  override def getSavePath: String = getStringParam("savePath")

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

  override def loadFrom(path: String): Model = {
    this.model = LogisticRegressionModel.load(path)
    this
  }

  override def setLoadPath(path: String): this.type = {
    addParam("loadPath","path")
    this
  }


  override def setSavePath(path: String): this.type = {
    addParam("savePath","path")
    this
  }

  override def run(data: Dataset[Row]): Unit = {
    val model = new LRModel()
    model.loadFrom(getLoadPath)

  }

  /**
    * 预测方法。通过模型对数据进行预测
    *
    * @param data 经过流程传入的数据
    * @return 训练结果
    */
  override def predict(data: Dataset[Row]): Dataset[Row] = {
    model.transform(data)
  }
}
