package com.haiyisoft.bds.flow

import org.apache.spark.sql.DataFrame

/**
  * Created by XingxueDU on 2018/3/19.
  */
trait GettableItem[T] extends Item[T]{
  private var gotten:DataFrame = null

  /**
    * Run的包装实现，如果生成结果被反复利用则缓存
    * @return 生成的DataFrame
    */
  def runAndGet:DataFrame= synchronized{
    if(gotten==null){
      gotten = run
      if(this.cacheLevel == Item.ALWAYS_CACHE)gotten.cache()
      gotten
    }else{
      if(this.cacheLevel == Item.CACHE_IF_RERUN)gotten.cache()
      gotten
    }
  }


  /**
    * run的具体实现，执行从其[[previous]]获取DataFrame然后由自身被包装的类进行处理并返回
    * 实现此特征的关键就是实现此方法
    * @return 处理生成的DataFrame
    */
  protected def run:DataFrame


}

/**
  * 如果对有复数入口的流程项调用previous方法则会自动生成此包装类。
  * 包装类可以正确的对群组进行设置、搜索与检查操作，但是无法执行或者getId
  *
  * 该类不应该由程序员手动创建实例
  *
  * @param list 所有子流程列表
  * @param father 生成该类的流程，该类的下游
  */
private[bds] class MultipleGettableItem(val list:Array[GettableItem[_]],val father:Item[_]) extends GettableItem[GettableItem[_]]{
  override def getFlowPlan: String = "MULTIPLE"

  /**
    * @deprecated 由于包装了复数操作，该类无法执行
    */
  override protected def run: DataFrame = throw new NoSuchMethodException()

  /**
    * @deprecated 虽然可以get，但是直能get到本身
    */
  override def get: GettableItem[_] = this

  /**
    * @deprecated 由于包装了复数操作，该类无对应id
    */
  override def getId: String = throw new NoSuchMethodException()

  /**
    * @deprecated 由于包装了复数操作，该类无法执行
    */
  override def runAndGet: DataFrame = throw new NoSuchMethodException()

  override def checkIntegrity:Array[Item[_]]={
    var res = Array[Item[_]]()
    if(this.list.contains(null)) res :+= father
    this.list.foreach{k=>
      if(k!=null)res++=k.checkIntegrity
    }
    res
  }


  override def search(id: String): Item[_] = {
    val s = this.list.flatMap(x=>Option[Item[_]](x.search(id)))
    if(s.isEmpty)null else s.head
  }

  override def setCacheLevel(cacheLevel:Int= Item.CACHE_IF_RERUN, applyForSubitems:Boolean): this.type ={
    this.list.foreach(_.setCacheLevel(cacheLevel,applyForSubitems))
    this
  }

  override def previous(): GettableItem[_] = {
    val s = this.list.map(_.previous()).distinct
    new MultipleGettableItem(s.toArray,this)
  }

  override def toString = s"[abs][flow]Multiple from "+father.toString

  override def equals(obj: scala.Any): Boolean = {
    obj.isInstanceOf[this.type]&&{
      val oth = obj.asInstanceOf[this.type]
      oth.father == this.father
    }

  }
}