package com.haiyisoft.bds.plugins.action.ml.regression

import com.haiyisoft.bds.api.action.ModelTraining
import com.haiyisoft.bds.api.data.Model
import com.haiyisoft.bds.api.data.trans.DataProcessInter
import com.haiyisoft.bds.api.param._
import com.haiyisoft.bds.api.param.col.HasOutputParam
import com.haiyisoft.bds.api.param.ml._
import org.apache.spark.ml.recommendation.{ALS => MLTraining, ALSModel => MLModel}
import org.apache.spark.sql.DataFrame

import scala.collection.mutable.ArrayBuffer

/**
  * Created by XingxueDU on 2018/3/22.
  */
class ALSAction extends ModelTraining[ALSModel] with DataProcessInter
  with HasMaxIterParam with HasRegParam with HasNotNecessaryParam with HasSeedParam with HasItemAndUserColParam
  with HasOutputParam{

  val als = new MLTraining()
  val RANK_KEY = "rank"
  val ALPHA_KEY = "alpha"
  val RATING_COL_KEY = "ratingCol"

  /**
    * 模型训练
    *
    * @param data 训练集
    * @return 生成的模型
    */
  override def fit(data: DataFrame): ALSModel = {
    if(hasSeed)als.setSeed(getSeed)
    als.setMaxIter(getMaxIter)
      .setItemCol(getItemCol)
      .setUserCol(getUserCol)
      .setRatingCol(getRatingCol)
      .setAlpha(getAlpha)
      .setCheckpointInterval(getCheckpointInterval)
      .setNonnegative(getNonNegative)
      .setRank(getRank)
      .setRegParam(getRegParam)
    val model = als.fit(data)
    new ALSModel(model)
  }

  /**
    * 变换的执行接口，从待变换的data转化为变换后的data
    *
    * @param data 待变换的表
    * @return 变换后的表
    */
  override def transform(data: DataFrame): DataFrame = {
    val model = fit(data)
    if(isWantSave)save(model)
    model.setItemCol(getItemCol)
      .setUserCol(getUserCol)
      .setOutputCol(getOutputCol)
      .transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    val maxIter = maxIterFromUser
    val reg = regParamFromUser
    val item = itemColFromUser
    val user = userColFromUser
    val rate = ratingColFromUser
    val alpha = alphaFromUser
    val ci = checkpointIntervalFromUser
    val nn = nonNegativeFromUser
    val rank = rankFromUser
    val out = ParamFromUser.String(OUTPUT_COL_KEY,"请输入输出列列名#[中间流程专用]输出列列名，如果只输出模型该参数可留空",true)
    //    val more = moreFromUser
    Array(user,item,rate,alpha,ci,nn,rank,maxIter,reg,out)
  }


  /**
    * 保存对象.注意：保存的是结果而不是自身
    *
    * @param stored 被保存对象（dataFrame或者Model）
    */
  override def save(stored: ALSModel): Unit = {
    stored.saveTo(getSavePath)
  }


  /////////////////
  ////HasHatingCol
  ////////////////
  def getRatingCol: String ={
    val res = $(RATING_COL_KEY)
    if(res==null){
      throw new IllegalArgumentException("should set Rating Col")
    }
    res
  }
  def setRatingCol(name: String): this.type = {
    addParam(RATING_COL_KEY,name)
    this
  }
  protected def ratingColFromUser:ParamFromUser={
    ParamFromUser.String(RATING_COL_KEY,s"请输入rating col#用户对商品的评价列列名",true)
  }

  /////////////////
  ////alpha
  ////////////////
  setAlpha(1d)
  def getAlpha: Double =getValueParam(ALPHA_KEY).doubleValue()
  def setAlpha(alpha: Double): this.type = {
    addParam(ALPHA_KEY,alpha)
    this
  }
  protected def alphaFromUser:ParamFromUser={
    val res = ParamFromUser.Double(ALPHA_KEY,"alpha for implicit preference")>=0
    res.fix.addDefaultValue("1")
  }

  //////////////////////////
  ////CheckpointInterval
  /////////////////////////
  final val CHECKPOINT_INTERVAL = "checkpointInterval"
  setCheckpointInterval(10)

  def getCheckpointInterval: Int =getValueParam(CHECKPOINT_INTERVAL).intValue()
  def setCheckpointInterval(c: Int): this.type = {
    addParam(CHECKPOINT_INTERVAL,c)
    this
  }
  protected def checkpointIntervalFromUser:ParamFromUser={
    val res = ParamFromUser.Integer(CHECKPOINT_INTERVAL,"设置检查点间隔#set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations")=== -1 ||>= 1
    res.fix.addDefaultValue("10")
  }

  //////////////////////
  /////nonnegative
  /////////////////////
  final val NON_NEGATIVE_KEY = "nonnegative"
  setNonNegative(false)

  def getNonNegative: Boolean =getBoolParam(NON_NEGATIVE_KEY)
  def setNonNegative(c: Boolean): this.type = {
    addParam(NON_NEGATIVE_KEY,c)
    this
  }
  protected def nonNegativeFromUser:ParamFromUser={
    val res = ParamFromUser.Boolean(NON_NEGATIVE_KEY,"whether to use nonnegative constraint for least squares")
    res.addDefaultValue("false")
  }

  ///////////////
  /////rank
  //////////////

  setRank(10)

  def getRank: Int =getValueParam(RANK_KEY).intValue()
  def setRank(rank: Int): this.type = {
    addParam(RANK_KEY,rank)
    this
  }
  protected def rankFromUser:ParamFromUser={
    val res = ParamFromUser.Integer(RANK_KEY,"rank of the factorization")>= 1
    res.fix.addDefaultValue("10")
  }

  override def getPluginName: String = "ALS推荐"
}
class ALSModel extends Model with HasOutputParam with HasItemAndUserColParam{

  var model:MLModel = null
  def this(mLModel: MLModel)={
    this()
    this.model = mLModel
  }
  private[action] def saveTo(path:String):this.type={
    this.model.write.overwrite().save(path)
    this
  }
  /**
    * 模型的读取方法
    *
    * @param path 读取地址
    * @return 模型，通常是本身，也可以重新构建一个
    */
  override def loadFrom(path: String): this.type = {
    model = MLModel.load(path)
    this
  }


  /**
    * 预测方法。通过模型对数据进行预测
    *
    * @param data 经过流程传入的数据
    * @return 训练结果
    */
  override def transform(data: DataFrame): DataFrame = {
    if(model==null)loadFrom(getLoadPath)
    model.setPredictionCol(getOutputCol)
      .setItemCol(getItemCol)
      .setUserCol(getUserCol)
    model.transform(data)
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  override def getNecessaryParamList: Array[ParamFromUser] = {
    getDefaultParamList.toArray
  }
  override def getPluginName: String = "ALS推荐模型"
}
trait HasItemAndUserColParam extends WithParam{
  val ITEM_COL_KEY = "itemCol"
  val USER_COL_KEY = "userCol"

  def getItemCol: String ={
    val res = $(ITEM_COL_KEY)
    if(res==null){
      throw new IllegalArgumentException("should set ITEM Col")
    }
    res
  }

  /**
    * 设置输出列
    *
    * @param name 输出列的列名
    */
  def setItemCol(name: String): this.type = {
    addParam(ITEM_COL_KEY,name)
    this
  }

  protected override def getDefaultParamList: ArrayBuffer[ParamFromUser]={
    val s = super.getDefaultParamList
    s.+=(itemColFromUser)
    s.+=(userColFromUser)
    s
  }
  protected def itemColFromUser:ParamFromUser={
    ParamFromUser.String(ITEM_COL_KEY,s"请输入item col#商品列列名",notEmpty = true)
  }

  def getUserCol: String ={
    val res = $(USER_COL_KEY)
    if(res==null){
      throw new IllegalArgumentException("should set USER Col")
    }
    res
  }

  /**
    * 设置输出列
    *
    * @param name 输出列的列名
    */
  def setUserCol(name: String): this.type = {
    addParam(USER_COL_KEY,name)
    this
  }
  protected def userColFromUser:ParamFromUser={
    ParamFromUser.String(USER_COL_KEY,s"请输入user col#用户列列名",notEmpty = true)
  }
}
