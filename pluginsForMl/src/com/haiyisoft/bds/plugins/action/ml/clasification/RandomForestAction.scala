package com.haiyisoft.bds.plugins.action.ml.clasification

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.param.ParamFromUser
import com.haiyisoft.bds.api.param.col.{HasFeaturesColParam, HasLabelColParam, HasOutputParam}
import com.haiyisoft.bds.api.param.ml.HasSeedParam
import org.apache.spark.ml.classification.{RandomForestClassificationModel => MLModel, RandomForestClassifier => MLTraining}
import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/22.
  */
class RandomForestAction extends ModelTraining[RandomForestModel] with HasLabelColParam with HasFeaturesColParam
  with HasSeedParam{
  private val training : MLTraining = new MLTraining()
  addParam("maxBins",32)
  addParam("maxDepth",5)
  addParam("numTrees",20)
  /**
    * 模型训练
    *
    * @param data 训练集
    * @return 生成的模型
    */
  override def fit(data: DataFrame): RandomForestModel = {
    training.setImpurity(getImpurity)
      .setMaxBins(getMaxBins)
      .setMaxDepth(getMaxDepth)
      .setNumTrees(getNumTrees)
      .setSeed(getSeed)
      .setFeaturesCol(getFeaturesCol)
      .setLabelCol(getLabelCol)
    val model = training.fit(data)
    new RandomForestModel().setModel(model)
  }

  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: RandomForestModel): Unit = {
    if(getSavePath != "DO NOT SAVE")
      stored.saveTo(getSavePath)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val features = featuresColFromUser
    val label = labelColFromUser
    val impurity = ParamFromUser.SelectBox("impurity","请选择增益算法#Criterion used for" +
      " information gain calculation (case-insensitive). Supported options:\"entropy\" and \"gini\"","gini","entropy")
    val seed = seedFromUser
    val MaxBins = ParamFromUser.Integer("maxBins","请输入最大箱数#Max number of bins for" +
      " discretizing continuous features.  Must be >=2 and >= number of categories for any" +
      " categorical feature.").>=(2).fix.addDefaultValue("32")
    val MaxDepth = ParamFromUser.Integer("maxDepth","请输入最大深度#Maximum depth of the tree. (>= 0)" +
      " E.g., depth 0 means 1 leaf node; depth 1 means 1 internal node + 2 leaf nodes.").>=(0).fix.addDefaultValue("5")
    val NumTrees = ParamFromUser.Integer("numTrees","请输入树的个数#Number of trees to train (>= 1)").>=(1).fix.addDefaultValue("20")
    Array(features,label,impurity,MaxBins,MaxDepth,NumTrees,seed)
  }

  def getMaxBins:Int = getValueParam("maxBins").intValue()
  def getMaxDepth:Int = getValueParam("maxDepth").intValue()
  def getNumTrees:Int = getValueParam("numTrees").intValue()
  def getImpurity:String ={
    val res = $("impurity")
    if(res!="gini"&&res!="entropy")"gini"
    else res
  }

  override def getPluginName: String = "随机森林"
}
class RandomForestModel extends Model with HasFeaturesColParam with HasOutputParam{
  private var model:MLModel = _
  def setModel(model:MLModel):this.type ={
    this.model = model
    this
  }
  private[clasification] def saveTo(path:String)={
    model.write.overwrite().save(path)
  }
  /**
    * 模型的读取方法
    * 这个方法用于将model本身加载为保存在path位置的模型
    * 这个方法应该在transform中被自身调用
    *
    * @param path 读取地址
    */
  override protected def loadFrom(path: String): RandomForestModel.this.type = {
    this.model = MLModel.load(path)
    this
  }

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    if(this.model==null)loadFrom(getLoadPath)
    this.model.transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray
  }

  override def getPluginName: String = "随机森林模型"
}