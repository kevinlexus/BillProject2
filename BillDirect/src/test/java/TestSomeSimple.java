import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;

import java.util.*;
import java.util.function.Function;

@Slf4j
public class TestSomeSimple {

    public class AddThree implements Function<Long, Long> {
        @Override
        public Long apply(Long aLong) {
            return null;
        }
    }

    /**
     * Тестирование lambda - функции
     */
    @Test
    public void testLambda() {
        Calendar cal = Calendar.getInstance();
        cal.add(Calendar.YEAR, -1);
        Date prevYear = cal.getTime();
        System.out.println(prevYear);
        System.out.println(new Date());

        Function<Long, Long> adder = new AddThree();
        Long result = adder.apply((long) 4);
        System.out.println("result = " + result);

        //
        Double inputDouble = 4.25D;
        int len = 7;
        String str = Utl.getMoneyStr(inputDouble, len, " ", "###,###.##");
        log.info(String.format("|%s|", str));
        log.info("double={}", inputDouble.toString());
    }

}
