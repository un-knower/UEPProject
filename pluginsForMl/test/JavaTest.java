import com.haiyisoft.bds.api.data.Converter;
import com.haiyisoft.bds.plugins.process.complex.DictionaryStat;
import scala.Enumeration;

/**
 * Created by Namhwik on 2018/4/16.
 */
public class JavaTest {
    public static void main(String[] args) {
        Enumeration.Value c = new DictionaryStat().getTransType();
        //SHUFFLE,TRANSFORM,SELECT,FREE_COMBINATION
        System.out.println(c == Converter.SHUFFLE() );
    }
}
