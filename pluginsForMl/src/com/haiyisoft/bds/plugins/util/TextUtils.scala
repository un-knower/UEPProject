package com.haiyisoft.bds.plugins.util

import com.haiyisoft.bds.api.param.Logging

import scala.collection.mutable.ArrayBuffer

/**
  * Created by DU Xingxue on 2017/6/19.
 *
  * @author DU Xingxue
  * @version 8.2.10
  */
private[bds] object TextUtils extends Logging{

  /**
    * 用于切分的代码，与单纯的替换相比不会影响引号内的数据
    * 用于例如s.split(codeToSplit(","))
 *
    * @param s 正常切分时用的那个
    */
  def codeToSafeSplit(s:String) = s + "(?=(?:[^\"]*\"[^\"]*\"[^\"]*)*$|[^\"]*$)"

  /**
    * 获取被各种括号，包括\"包裹的内容，如果没有括号会直接返回原文
    * 注意：左括号必须是第一个字符
    *
    * @param code 带括号的文章
    * @param left 左括号样式
    * @param right 右括号样式
    * @return 括号内的文字
    */
  def getWordsInString(code:String,left:Char = '"',right:Char ='"'):String ={
//    logDebug("getWordsInString : "+code)
    val a = code.trim
    if(a(0)!=left)a
    else {
//      logDebug("    => "+a.substring(1,a.length-1))
      val idx = code.indexOf(right,1)
      assert(idx>0,s"$code has left[$left] but hasn't [$right]")
      a.substring(1,idx)
    }
  }


  /**
    * 获取成对括号的另一半，括号最好不要是全角
    *
    * @param left 左括号
    * @return 右括号
    */
  def couple(left:Char):Char ={left match{
    case '（'|'(' |'【'|'《'=> (left+1).toChar
    case '"' => '"'
    case '\'' => '''
    case _ => (left+2).toChar
  }}

  /**
    * 获取各种括号内的内容括号可以是各种但是最好不要全角
    *
    * @param code 以左括号开始的文章
    * @return (括号内的内容（包括括号），剩下的内容)
    */
  def getCouple(code:String):(String,String) ={
    logDebug("getCouple:"+code)
    var lv = 1
    var index = 1
    var inString = false
    val Left:Char = code.trim()(0)
    if(Left>='0'&&Left<='9')
    {
      index = code.indexOf(',')
      if(index == -1)index = code.length
    }
    else {
      val Right: Char = couple(Left)
      while (lv > 0 && index < code.length) {
        code(index) match {
          case Right if !inString => lv -= 1
          case Left if !inString => lv += 1
          case '"' =>
            val now = inString
            inString = !now
          case _ => ()
        }
        index += 1
      }
    }
    logDebug("    ["+index+"]=>")
    logDebug(code.substring(0,index))
    (code.substring(0,index),code.substring(index))
  }

  /**
    * 将一组key:val值组合成一个json字符串
    * 如果val可以被解析成string则组成 "key" : "val"串
    * 如果val可以被解析成json组则组成 "key" : {val} 串
    */
  def tojson(key:String,value:Any)=if(value.toString.startsWith("{")) "\""+key+"\":" + value  else "\""+key+"\":\"" + value + "\""

  /**
    * 将一组key：val组组成一个json字符串组
    * val中可以有别的组存在
    */
  def tojsons(att:(String,Any)*) = {
    val s = "{"+(""/:att)((a,b)=>a+tojson(b._1,b._2)+",")
    if(s.endsWith(","))s.substring(0,s.length-1)+"}"else "{}"
  }

  /**
    * 将str从endIdx开始往前取length个字符组成字符串
    */
  def substringFromEnd(str:String,endIdx:Int = 0,length:Int = -1): String ={
    if(length == -1)str.substring(0,str.length-endIdx)
    else str.substring(str.length-length-endIdx,str.length-endIdx)
  }

  /**
    * 将字符串根据长度进行分割
    * 对Unicode（宽字符）的支持度可能不够高
    *
    * @param input 输入的字符串
    * @param length 给定的分割长度
    * @param safeWords 分割时是否在意换行符和空格
    *                   false:通常用于分割数据流，单纯的按照长度分割
    *                   true :通常用于显示（自动换行）,对空格和回车敏感
    * @return 存有固定长度的字符串组的集合
    */
  def splitStringByLength(input:String,length:Int,safeWords:Boolean=false): Array[String] ={
    if(input.length<=length)Array(input)
    else if(!safeWords){
      val res = scala.collection.mutable.ArrayBuffer[String]()
      val remainder =  input.length % length
      val number = (BigDecimal(input.length)/length).setScale(0,BigDecimal.RoundingMode.DOWN).toInt
      for(index <- 0 until number){
        val childStr = input.substring(index*length,(index+1)*length)
        res+=childStr
      }
      if(remainder>0){
        val cStr = input.substring(number * length,input.length)
        res+=cStr
      }
      res.toArray
    }
    else{
      val res = scala.collection.mutable.ArrayBuffer[String]()
      var tmp = ""
      val spliter = {
        val s = System.getProperty("line.separator")
        if(input.contains(s))s else if(input.contains("\n"))"\n" else ""
      }
      var idx = 0
      var nextReturn = -1
      /** 当前行缓存追加in*/
      def append(in:String)={
        if(tmp.isEmpty)tmp = in
        else tmp+=" "+in
      }
      /**缓存入栈*/
      def toNext()={
        if(tmp.nonEmpty){
          res+=tmp
          tmp=""
        }
      }
      /**检查回车符*/
      def checkReturn()={
        if(spliter.nonEmpty&&nextReturn<idx){
          nextReturn = input.indexOf(spliter,idx)
          if(nextReturn == -1) nextReturn = Int.MaxValue
        }
      }
      /**检查下一个单词*/
      def checkWord()={
        val tp = {
          val resIdx = input.indexOf(" ",idx)
          if(resIdx == -1)input.length
          else resIdx min nextReturn
        }
        val wordLength = tp-idx
        val ttmp = input.substring(idx,tp)
        if(tp>=nextReturn){
          if(wordLength+tmp.length+1>length){
            toNext()
          }
          append(ttmp)
          toNext()
          idx = nextReturn+1
        }
        else if(wordLength>length){
          var st = idx+length-tmp.length-1
          do{
            append(input.substring(idx,st))
            toNext()
            idx = st
            st = st+length min tp
          }while(idx<tp)
        }else{
          if(wordLength+tmp.length+1>length){
            toNext()
          }
          append(ttmp)
          idx = tp+1
        }
      }
      val finalIdx = input.length-1

      while(idx<finalIdx){
        checkReturn()
        checkWord()
      }
      toNext()
      res.toArray
    }
  }
  /**
    * 将text包装成固定宽度length的string，如果text本身过长则返回text本身，如果text过短则用other补位
    * 如 5,"0","_"会被包装成 "____0"
    *
    * @param length 预定的字符串长度
    * @param text 被包装的文字
    * @param other 补位的文字
    */
  def mkTableCoreString(length:Int,text:String,other:String,left:Boolean = true): String ={
    if(left){
      if(other.length>0)other*((length-text.length)/other.length)+text else text
    }else{
      if(other.length>0)text+other*((length-text.length)/other.length) else text
    }
  }

  def isNumber(str:String):Boolean={
    var resDot = true
    (true/:str)((s,t)=>if(t < '0'||t > '9'){if(t=='.'&&resDot){resDot=false; s}else false} else s)
  }

  /** 7位ASCII字符，也叫作ISO646-US、Unicode字符集的基本拉丁块 */
  val US_ASCII = "US-ASCII"
  def toUS_ASCII = changeCharset(US_ASCII) _
  /** ISO 拉丁字母表 No.1，也叫作 ISO-LATIN-1 */
  val ISO_8859_1 = "ISO-8859-1"
  def toISO_8859_1 = changeCharset(ISO_8859_1) _
  /** 8 位 UCS 转换格式 */
  val UTF_8 = "UTF-8"
  def toUTF_8 = changeCharset(UTF_8) _
  /** 16 位 UCS 转换格式，Big Endian（最低地址存放高位字节）字节顺序 */
  val UTF_16BE = "UTF-16BE"
  def toUTF_16BE = changeCharset(UTF_16BE) _
  /** 16 位 UCS 转换格式，Little-endian（最高地址存放低位字节）字节顺序 */
  val UTF_16LE = "UTF-16LE"
  def toUTF_16LE = changeCharset(UTF_16LE) _
  /** 16 位 UCS 转换格式，字节顺序由可选的字节顺序标记来标识 */
  val UTF_16 = "UTF-16"
  def toUTF_16 = changeCharset(UTF_16) _
  /** 中文超大字符集 */
  val GBK = "GBK"
  def toGBK = changeCharset(GBK) _
/**
  * 字符串编码转换的实现方法
  *
  * @param str
  *            待转换编码的字符串
  * @param newCharset
  *            目标编码
  */
  def changeCharset(newCharset:String)(str:String): String ={
    if(str==null||str.length==0)str
    // 用默认字符编码解码字符串,用新的字符编码生成字符串
    else new String(str.getBytes(),newCharset)
  }
  /**
    * 字符串编码转换的实现方法
    *
    * @param str
    *            待转换编码的字符串
    * @param oldCharset
    *            原编码
    * @param newCharset
    *            目标编码
    */
  def changeCharset(oldCharset:String,newCharset:String)(str:String): String ={
    if(str==null||str.length==0)str
    // 用旧的字符编码解码字符串。解码可能会出现异常。
    else new String(str.getBytes(oldCharset),newCharset)
  }
  private def getShi(address:String):String={
    var res = ""
    if(address.contains("直辖县"))res="省直辖县级行政区划"
    else if(address.substring(0,address.length.min(5)).contains("市"))res=address.split("市")(0)+"市"
    else if(address.substring(0,address.length.min(10)).contains("自治州"))res=address.split("自治州")(0)+"自治州"
    else if(address.substring(0,address.length.min(7)).contains("地区"))res=address.split("地区")(0)+"地区"
    res
  }

  def getPlace(address:String):Array[String]={
    val res = ArrayBuffer("","")
    address match{
      case x if x.startsWith("内蒙")=>
        res(0)="内蒙古自治区"
        val other = address.split("区",2)(1)
        if(other.startsWith("兴安"))res(1)="兴安盟"
        else if(other.startsWith("锡林"))res(1)="锡林郭勒盟"
        else if(other.startsWith("阿拉善"))res(1)="阿拉善盟"
        else res(1)=getShi(other)
      case x if x.startsWith("广西")=>
        res(0)="广西壮族自治区"
        val other = address.split("区",2)(1)
        getShi(other)
      case x if x.startsWith("西藏")=>
        res(0)="西藏自治区"
        val other = address.split("区",2)(1)
        if(other.startsWith("那曲"))res(1)="那曲地区"
        else if(other.startsWith("阿里"))res(1)="阿里地区"
        else res(1)=getShi(other)
      case x if x.startsWith("宁夏")=>
        res(0)="宁夏回族自治区"
        val other = address.split("区",2)(1)
        res(1)=getShi(other)
      case x if x.startsWith("新疆")=>
        res(0)="新疆维吾尔自治区"
        val other = address.split("区",2)(1)
        if(other.contains("自治州"))res(1)=address.split("自治州",2)(1)+"自治州"
        else if(other.substring(0,5).contains("地区"))res(1)=address.split("地区",2)(1)+"地区"
        else if(other.contains("直辖县"))res(1)="自治区县级行政规划"
        else getShi(other)
      case x if x.startsWith("香港")=>res(0)="香港特别行政区"
      case x if x.startsWith("澳门")=>res(0)="澳门特别行政区"
      case x if x.startsWith("北京")=>res(0)="北京市"
      case x if x.startsWith("天津")=>res(0)="天津市"
      case x if x.startsWith("上海")=>res(0)="上海市"
      case x if x.startsWith("黑龙江省")=>
        res(0) = "黑龙江省"
        val other = address.split("省",2)(1)
        if(other.startsWith("大兴安岭"))res(1)="大兴安岭地区"
        else getShi(other)
      case x if x(2)=='省'=>{
        res(0)=address.substring(0,3)
        val other = address.substring(3)
        res(0) match{
          case "吉林省"|"湖北省"|"湖南省"|"四川省"|"贵州省"|"云南省"|"甘肃省"|"青海省" if other.contains("自治州")=>res(1)=other.split("自治州",2)(0)+"自治州"
          case _ =>res(1)=getShi(other)
        }
      }
      case _ => res(1)=getShi(address)
    }
    res.toArray
  }
  /**
    * 将字符串转成unicode
    *
    * @param str
    *            待转字符串
    * @return unicode字符串
    */
  def covert(str:String):String={
    if(str==null||str.length==0)str
    else{
      val sb = new StringBuffer(1000)
      sb.setLength(0)
      for(x<-str){
        sb.append("\\u")
        val j = x >>> 8 // 取出高8位
        val tmp = Integer.toHexString(j)
        if(tmp.length == 1)sb.append("0")
        sb.append(tmp)
        val k = x & 0xFF // 取出低8位
        val tmp2 = Integer.toHexString(k)
        if(tmp2.length==1)sb.append("0")
        sb.append(tmp2)
      }
      new String(sb)
    }
  }
}
