import com.haiyisoft.bds.plugins.process.complex.DictionaryStat

/**
  * Created by Namhwik on 2018/4/16.
  */
object Test {
  def main(args: Array[String]): Unit = {

    //val as: Converter.Value = new DictionaryStat().getTransType
    //SHUFFLE,TRANSFORM,SELECT,FREE_COMBINATION
    val ds = new DictionaryStat()
    ds.getNecessaryParamList.foreach(
      x=>println(x.getName+"=>"+x.getTypeName)
    )
  }
}
