package com.haiyisoft.bds.flow

import java.util.logging.Logger

import com.haiyisoft.bds.api.action.Action
import com.haiyisoft.bds.api.data.{DataProcessorInter, DataLoadInter}

import scala.collection.mutable


/**
  * 构建器
  *
  * 构建方法类似于
  * newBuilder()
  * .append(ActionItem(action,myId),nextId)
  * .append ...
  * .result()
  *

  *@throws IllegalStateException:发现有Id重复、构建时流程结构不完整时抛出
  *@throws ClassCastException:当一个Item被放置在不可能的位置（如action被放置在流程中间而非末尾）时抛出
  *@throws IllegalArgumentException:参数错误，如未给一个action提供唯一的lastId时抛出
  * Created by XingxueDU on 2018/3/19.
  */
class Builder{
  protected var action:ActionItem[_] = null
  protected var lastId:String = null
  protected val DataProcessors = mutable.HashMap[String,Array[(Int,DataProcessorItem[_])]]()
  protected val mapById = mutable.HashMap[String,Item[_]]()

  def append(it:Item[_],lastId:String*): this.type ={
    val id = it.getId
    if(mapById.contains(id)){
      if(! (mapById(id) equals it))throw new IllegalStateException(s"Multiple item has a same id[$id]: \n\t->[$it]\n\t->[${mapById(id)}]")
      Builder.logging.warning(s"a same item[$id] has append multiple times")
    }
    mapById.+=(id->it)
    it match{
      case ac:ActionItem[_]=>
        if(action!=null&& action.getId!= id) throw new IllegalStateException(s"try to met Multiple Action with id = [$id] and [${action.getId}]")
        action = ac
        if(lastId.isEmpty)throw new IllegalArgumentException(s"last id shouldn't be empty for Action[$id]")
        this.lastId = lastId.head
      case dl:DataLoaderItem[_]=>
        if(lastId.nonEmpty)throw new IllegalArgumentException(s"last id should be empty for DataLoader[$id]")
      case dp:DataProcessorItem[_]=>
        if(lastId.isEmpty)throw new IllegalArgumentException(s"last id shouldn't be empty for DataProcessor[$id]")
        lastId.zipWithIndex.foreach{ case (last,idx)=>
          val old = DataProcessors.getOrElse(last,null)
          if(old==null){
            DataProcessors.+=(last->Array(idx->dp))
          }
          else{
            old.foreach{case(oid,odp)=>
              if(oid==idx&&odp==dp){
                Builder.logging.warning(s"a same item[$id] want to registered the same port $lastId.$idx")
                return this
              }
            }
            DataProcessors.+=(last->old.:+(idx->dp))
          }
        }
    }
    this
  }
  def appendDataLoader[T<:DataLoadInter](dataLoad:T,id:String)={
    append(new DataLoaderItem[T](dataLoad,id))
  }
  def appendProcessor[T<:DataProcessorInter](processor:T,id:String,lastId:String*)={
    append(new DataProcessorItem[T](processor,id),lastId:_*)
  }
  def appendAction[T<:Action[_]](action:T,id:String,lastId:String)={
    append(new ActionItem[T](action,id),lastId)
  }

  private def search(id:String):Item[_] =mapById.getOrElse(id,null)

  private def buildAction():Unit={
    if(action==null)throw new IllegalStateException("No Action Found")
    val last = search(lastId)
    if(last==null)throw new IllegalStateException(s"No Item Found for id $lastId in plan () -> Action")
    last match{
      case a:DataProcessorItem[_]=>
        action.setPrevious(a)
      case b:DataLoaderItem[_]=>
        action.setPrevious(b)
      case _ =>
        throw new ClassCastException(s"previous of Action should be a GettableItem but not ${last.getClass}")
    }
  }
  private def buildGettableItem():Unit={
    DataProcessors.foreach{case ((last_id,dxx))=>
      val last = search(last_id)
      if(last == null)throw new IllegalStateException(s"No Item Found for id $last in plan () -> ${dxx.head._2}")
      last match{
        case a:DataProcessorItem[_]=>
          dxx.foreach{case((port,po))=>
            po.addPrevious(port,a)
          }
        case b:DataLoaderItem[_]=>
          dxx.foreach{case((port,po))=>
            po.addPrevious(port,b)
          }
        case _ =>
          throw new ClassCastException(s"id[$last_id] should be a GettableItem but not ${last.getClass}")
      }
    }
  }

  def result():ActionItem[_]={
    buildGettableItem()
    buildAction()
    val checked = action.checkIntegrity.map(_.toString)
    if(checked.nonEmpty)throw new IllegalStateException("Failed to pass integrity check:\n\t->"+checked.mkString("\n\t->"))
    action
  }
}
object Builder {
  def newBuilder() = new Builder()
  private[Builder] val logging = Logger.getLogger("com.haiyisoft.bds.flow.Builder")

  //Test
//  def main(args: Array[String]) {
//    val join = new DataJoin {
//      override def join(fstTable: DataFrame, sndTable: DataFrame): DataFrame = null
//
//      override def containsDefaultParam(key: String): Boolean = false
//
//      override def getNecessaryParamList: Array[ParamFromUser] = null
//
//      override def getDefaultParam(key: String): Any = null
//    }
//    val transformer = new Udf2DataMining(null)
//    newBuilder()
//      .append(DataProcessorItem(transformer,"E"),"F")
//      .append(DataProcessorItem(join,"T"),"C","E")
//      .append(DataLoaderItem(null,"D"))
//      .append(DataProcessorItem(transformer,"C"),"D")
//      .append(DataLoaderItem(null,"F"))
//      .append(ActionItem(null,"A"),"T")
//      .result()
//  }
}
