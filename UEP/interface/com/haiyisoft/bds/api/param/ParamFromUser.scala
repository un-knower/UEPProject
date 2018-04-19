package com.haiyisoft.bds.api.param

/** 用于[[WithParam.getNecessaryParamList]]方法，实现向前端要求参数
  *
  * 方法包含3个部分：
  * *name：用于提交给Param的key
  * *description：用于提示用户输入的字符串
  * *checkRule：用于判断用户输入是否合法的检查类
  *
  * 该类不用于承接用户输入的参数，也不用于将用户输入的参数转化为合法类型
  * 参数的获取及结果的返回应该分别通过[[WithParam.getNecessaryParamList]]和[[WithParam.addParam()]]方法进行
  *
  * Created by XingxueDU on 2018/3/17.
  */
class ParamFromUser {
  def this(name: String, description: String = ParamFromUser.DEFAULT, checkRule: String) = {
    this()
    this.name = name
    if (this.description == ParamFromUser.DEFAULT) this.description = "请输入" + name
    else this.description = description
    this.checkRule = checkRule
  }

  override def equals(obj: Any): Boolean = {
    obj.isInstanceOf[this.type] && {
      val o = obj.asInstanceOf[this.type]
      o.name == this.name
    }
  }

  /**
    * 输入到param中的key
    */
  private var name: String = ""
  /**
    * 显示给用户提示输入的描述
    */
  private var description: String = "请输入参数" + name
  /**
    * 判断用户输入是否合法的规则
    */
  private var checkRule: String = "function check(a) { \n return true; \n}check(\"#value#\")"
  /**
    * 默认值
    */
  private val defaultValue = scala.collection.mutable.ArrayBuffer[String]()
  /**
    * 类型描述，getClass.getName.toLowerCase()
    */
  private var tp: String = "string"

  /**
    * 是否允许为该参数传入多条数据
    */
  private var allowMultipleLineGet = false

  /**
    * 是否需要Schema信息
    *
    * @param needSchema :Boolean
    * @return
    */
  private var needSchema: Boolean = false

  /**
    * 参数的中文名称，供前台展示使用
    */
  private var shortName: String = name

  def setShortName(shortName: String): this.type = {
    this.shortName = shortName
    this
  }

  def getShortName: String = shortName

  def setNeedSchema(flag: Boolean): this.type = {
    needSchema = flag
    this
  }

  def getNeedSchema: Boolean = needSchema

  def setTypeName(tp: String): this.type = {
    this.tp = tp
    this
  }

  def setInputTypeName(): ParamFromUser.this.type = setTypeName("inputCol")

  def setOutputTypeName(): ParamFromUser.this.type = setTypeName("outputCol")

  def getDescription: String = description

  def getName: String = name

  def getTypeName: String = tp

  def isSelectBox: Boolean = defaultValue.size > 1

  def getDefaultValue: Array[String] = {
    val res = defaultValue.distinct.toArray
    if (res.length == defaultValue.length) res
    else {
      defaultValue.groupBy(s => s).toSeq.sortBy(_._2.length * (-1)).map(_._1).toArray
    }
  }


  override def toString: String = name

  def addDefaultValue(value: String*): this.type = {
    defaultValue ++= value
    this
  }

  def setParamName(name: String): this.type = {
    this.name = name
    this
  }

  def setDescription(des: String): this.type = {
    this.description = des
    this
  }

  def setCheckRule(rule: String): this.type = {
    this.checkRule = rule

    this
  }

  def checkJsFunction: String = checkRule

  def setAllowMultipleResult(allow: Boolean): this.type = {
    this.allowMultipleLineGet = allow
    this
  }

  def getAllowMultipleResult: Boolean = {
    this.allowMultipleLineGet
  }
}

object ParamFromUser {
  val DEFAULT = "\0"

  def apply(name: String, description: String = DEFAULT, rule: String): ParamFromUser = {
    new ParamFromUser(name, description, rule)
  }

  def String(name: String, description: String = DEFAULT, notEmpty: Boolean = false): ParamFromUser = {
    val rule = if (notEmpty) {
      "function check(a){return a.length>0;}check(\"#value#\")"
    } else {
      "function check(a){return true;}check(\"#value#\")"
    }
    new ParamFromUser(name, description, rule)
  }

  def Number(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description)

  def Integer(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description).more("!parseInt(a)===t").setTypeName("int")

  def Long(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description) >= "-2147483648" <= "2147483647" more "t%1===0"

  def Boolean(name: String, description: String = DEFAULT): ParamFromUser = {
    val rule = "function check(a){var t = a.toLowerCase();return a==\"true\"||a==\"false\";}check(\\\"#value#\\\")"
    new ParamFromUser(name, description, rule).setTypeName("boolean").addDefaultValue("true").addDefaultValue("false").setTypeName("boolean")
  }

  def Double(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description).setTypeName("double")

  def Float(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description) more "!isNaN(parseFloat(a))" setTypeName "float"

  def BigDecimal(name: String, description: String = DEFAULT) = new NumParamBuilder(name, description)

  def SelectBox(name: String, description: String, optional: String*): ParamFromUser = {
    val list = optional.mkString("new Array(\"", "\",\"", "\");")
    val rule = "function check(a){var array = " + list + "return array.indexOf(a)>=0;}check(\"#value#\")"
    ParamFromUser(name, description, rule).addDefaultValue(optional: _*)
  }

  object save {
    def mode(): ParamFromUser = {
      val optional = Array("append", "overwrite", "ignore", "error")
      val list = optional.mkString("new Array(\"", "\",\"", "\");")
      val rule = "function check(a){var array = " + list + ";return array.indexOf(a)>=0;}check(\"#value#\")"
      ParamFromUser
        .apply(ParamFromUser.variables.saveMode,
          "当文件已存在时的处理方式，append追加，overwrite覆盖，ignore不再保存，error以错误弹出",
          rule)
        .addDefaultValue(optional: _*)
    }

    def savePath(description: String = "请输入保存路径"): ParamFromUser = {
      ParamFromUser.String(ParamFromUser.variables.savePath, description, notEmpty = false).addDefaultValue("DO NOT SAVE")
    }

    def loadPath(description: String = "请输入数据读取路径"): ParamFromUser = {
      ParamFromUser.String(ParamFromUser.variables.loadPath, description, notEmpty = false)
    }
  }

  object variables {
    val inputColName = "inputCol"
    val outputColName = "outputCol"
    val featuresColName = "featuresCol"
    val labelColName = "labelCol"
    val probabilityColName = "probabilityCol"
    val savePath = "param.save.path"
    val loadPath = "param.load.path"
    val saveMode = "param.save.mode"
    val seed = "seed"
    val more = "param.var.more"
    /**
      * Param for the convergence tolerance for iterative algorithms.
      */
    val tol = "tol"
    /**
      * Param for maximum number of iterations (>= 0).
      */
    val maxIter = "maxIter"
    /**
      * Param for regularization parameter (>= 0).
      */
    val regParam = "regParam"
    /**
      * Param for threshold in binary classification prediction, in range [0, 1].
      */
    val threshold = "threshold"
    /**
      * Param for set checkpoint interval (>= 1) or disable checkpoint (-1). E.g. 10 means that the cache will get checkpointed every 10 iterations.
      */
    val checkpointInterval = "checkpointInterval"
    /**
      * Param for whether to fit an intercept term.
      */
    val fitIntercept = "fitIntercept"
    /**
      * Param for how to handle invalid entries. Options are skip (which will filter out rows with bad values), or error (which will throw an error). More options may be added later.
      */
    val handleInvalid = "handleInvalid"
    val separator = '@'
  }

}

