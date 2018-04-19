package com.haiyisoft.bds.api.param

import java.util

import com.amazonaws.util.json.JSONArray
import com.haiyisoft.bds.api.data.Converter

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.util.Try

/** 继承此特性的类表示其有可以/需要设置的参数
  *
  * 由通过继承其他特性的方式被要求实现的setter/getter方法请尽量以paramMap作为储存器
  *
  *
  * Created by XingxueDU on 2018/3/15.
  *
  * @version 0.0.5 build 5
  */
trait WithParam extends Logging {
  protected val paramMap: util.HashMap[String, Any] = new util.HashMap[String, Any]

  //插件默认名字
  def getPluginName: String

  def getTransType: Converter.Value = Converter.TRANSFORM

  def combinationSchema(): Array[String] = Array()

  def getSchema(columns: String*): Array[String] = {
    import Converter._
    val transType = getTransType
    val input: Option[ParamFromUser] = getNecessaryParamList.find(x => x.getTypeName.equals("inputCol"))
    val output = getNecessaryParamList.find(x => x.getTypeName.equals("outputCol"))

    def getOptionParam(param: Option[ParamFromUser]): Array[String] = {
      if (param.nonEmpty) {
        val paramFromUser = param.get
        if (paramFromUser.getAllowMultipleResult) {
          jsonArr2Arr(getParam(paramFromUser.getName).toString) //getParam(paramFromUser.getName).toString.split('@')
        }
        else
          Array(getParam(paramFromUser.getName).toString)
      }
      else
        Array()
    }

    transType match {
      case TRANSFORM => columns.toArray ++ getOptionParam(output)
      case SHUFFLE => getOptionParam(input) ++ getOptionParam(output)
      case SELECT => getOptionParam(input)
      case FREE_COMBINATION => combinationSchema()
    }
  }


  def multiParam2Str(params: String*): String = {
    val res = new JSONArray
    params.foreach(res.put)
    res.toString()
  }

  def jsonArr2Arr(jSONArray: String): Array[String] = {
    val jArr = new JSONArray(jSONArray)
    (0 until jArr.length()).map(jArr.getString).toArray
  }

  def checkSchema(lastColumns: Array[String], toCheckedColumns: Array[String]): Boolean =
    lastColumns sameElements getSchema(lastColumns: _*)

  def getParamList: Array[(String, Any)] = {
    import scala.collection.JavaConversions._
    val s = paramMap.keySet()
    s.foreach(s.add)
    val d = Array.newBuilder[String]
    val dfKeys = (getNecessaryParamList.map(_.getName) ++ d.result()).distinct
    dfKeys.map(x => (x, getParam(x)))
  }

  /**
    * 获取参数，设置参数优先，没有则取默认参数
    *
    * @param key 参数名
    * @return 参数值
    */
  def getParam(key: String): Any = {
    if (paramMap.containsKey(key)) paramMap.get(key) else null
  }

  def getStringParam(key: String): String = {
    val s = getParam(key)
    if (s != null) s.toString
    else null
  }

  def getArrayStringParam(key: String): Array[String] = {
    val value = getParam(key)
    if (value == null) null
    else {
      val s = Try {
        value match {
          case as: Array[String] => as
          case s: String => jsonArr2Arr(s)
          case ts: TraversableOnce[_] => ts.map(_.toString).toArray
          case js: java.util.Collection[_] =>
            val builder = Array.newBuilder[String]
            val it = js.iterator()
            while (it.hasNext) builder += it.next().toString
            builder.result()
          case oth => Array(oth.toString)
        }
      }
      if (s.isFailure) {
        throw new ClassCastException(s"can't analyse $value[${value.getClass}] as a Array[String]")
      }
      s.get
    }
  }

  def getArrayDoubleParam(key: String): Array[Double] = {
    val value = getParam(key)
    if (value == null) null
    else {
      val s = Try {
        value match {
          case as: Array[Double] => as
          case s: String => jsonArr2Arr(s).map(_.toDouble)
          case ts: TraversableOnce[_] => ts.map(_.toString.toDouble).toArray
          case js: java.util.Collection[_] =>
            val builder = Array.newBuilder[Double]
            val it = js.iterator()
            while (it.hasNext) builder += it.next().toString.toDouble
            builder.result()
          case mlv: org.apache.spark.ml.linalg.Vector => mlv.toArray
          case libv: org.apache.spark.mllib.linalg.Vector => libv.toArray
        }
      }
      if (s.isFailure) {
        throw new ClassCastException(s"can't analyse $value[${value.getClass}] as a Array[Double]")
      }
      s.get
    }
  }

  def getValueParam(key: String): BigDecimal = {
    val value = getParam(key)
    if (value == null) null
    else {
      val s = Try {
        value match {
          case b: BigDecimal => b
          case str: String => BigDecimal(str)
          case d: Double => BigDecimal(d)
          case f: Float => BigDecimal(f)
          case b: Byte => BigDecimal(b)
          case s: Array[Byte] => BigDecimal(String.valueOf(s.map(_.toChar)))
          case l: Long => BigDecimal(l)
          case s: Short => BigDecimal(s)
          case i: Int => BigDecimal(i)
          case c: Char => BigDecimal(c)
          case ac: Array[Char] => BigDecimal(ac)
          case inf: Any => BigDecimal(inf.toString)
        }
      }
      if (s.isFailure) {
        throw new ClassCastException(s"can't analyse $value[${value.getClass}] as a value")
      }
      s.get
    }
  }

  def getBoolParam(key: String): Boolean = {
    val s = getParam(key)
    if (s == null) throw new NullPointerException("undefined param " + key)
    val t = s match {
      case s: String => s.toLowerCase().toBoolean
      case b: Boolean => b
      case i: Int => i != 0
      case _ => throw new ClassCastException(s.getClass + " can't cast to Boolean")
    }
    t
  }

  /**
    * 添加一条参数
    *
    * @param key   参数名
    * @param value 参数值
    */
  def addParam(key: String, value: Any): this.type = {
    this.paramMap.put(key, value)
    this
  }

  def addAllParams(params: Map[String, Any]): this.type = {
    import scala.collection.JavaConversions._
    this.paramMap.putAll(params)
    this
  }

  /**
    * 获取该类必须设置的参数列表
    *
    * @return 参数列表
    */
  def getNecessaryParamList: Array[ParamFromUser]

  protected def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    new ArrayBuffer[ParamFromUser]()
  }

  def checkNecessaryParam: Boolean = {
    getNecessaryParamList.map(_.getName).map { name =>
      getParam(name) != null
    }.forall(z => z)
  }


  /**
    * @see [[WithParam.getParam()]]
    */
  protected def $(key: String) = getStringParam(key)
}

trait HasNotNecessaryParam extends WithParam {
  final val MORE_PARAM_KEY = ParamFromUser.variables.more

  /**
    * 添加一条参数
    *
    * @param key   参数名
    * @param value 参数值
    */
  override def addParam(key: String, value: Any): this.type = {
    super.addParam(key, value)
    if (key != null && key == MORE_PARAM_KEY) {
      val params = getArrayStringParam(MORE_PARAM_KEY)
      params.foreach { paramCode =>
        val s = paramCode.split(",")
        if (s.length == 2) {
          super.addParam(s.head, s(1))
        }
      }
    }
    this
  }

  def getNotNecessaryParams: Map[String, String] = {
    getArrayStringParam(MORE_PARAM_KEY).flatMap { str =>
      val res = str.split(",")
      var r2 = res(1).trim
      if (r2.startsWith("\"")) r2 = r2.substring(1, r2.length - 1)
      if (res.length == 2) Option((res.head, r2))
      else None
    }.toMap
  }

  protected val moreParamList = ArrayBuffer[String]()

  protected def setMoreParamList(param: String*): this.type = {
    moreParamList.clear()
    moreParamList ++= param
    this
  }

  protected def addMoreParam(param: ParamFromUser*): this.type = {
    param.foreach(addOneMoreParam)
    this
  }

  protected def addOneMoreParam(param: ParamFromUser): this.type = {
    val key = param.getName
    val des = param.getDescription
    moreParamList += s"$key : $des"
    this
  }

  def moreFromUser: ParamFromUser = {
    val rule = "function check(a){return a.length==0||a.split(',').length==2;}check(\"#value#\")"
    var description = "请输入自定义参数并以逗号[半角]分割。没有参数时请留空。"
    if (moreParamList.nonEmpty) description += moreParamList.mkString("允许使用的param包括：", ", ", "。")
    ParamFromUser(ParamFromUser.variables.more, description, rule)
  }
}

trait HasLoadParam extends WithParam {
  final val LOAD_PATH_KEY = ParamFromUser.variables.loadPath

  /**
    * 设置读取地址，读取保存的数据。
    */
  def setLoadPath(path: String): this.type = {
    addParam(LOAD_PATH_KEY, path)
  }

  /**
    * 返回读取地址，从该地址加载数据
    */
  def getLoadPath: String = getStringParam(LOAD_PATH_KEY)

  override protected def getDefaultParamList: ArrayBuffer[ParamFromUser] = {
    val old = super.getDefaultParamList
    old += ParamFromUser.save.loadPath()
    old
  }
}


