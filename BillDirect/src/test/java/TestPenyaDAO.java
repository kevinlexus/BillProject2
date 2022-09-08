import com.dic.app.config.Config;
import com.dic.bill.dao.PenyaDAO;
import com.dic.bill.dto.SumFinanceFlow;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.annotation.Rollback;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestPenyaDAO {

    @Autowired
    private PenyaDAO penyaDAO;

    /**
     * Проверка projection для перерасчетов
     */
    @Test
    @Rollback(value = true)
    public void checkProjectionFinanceFlow() {
        log.info("-----------------Begin");
        List<SumFinanceFlow> lst = penyaDAO.getFinanceFlowByKlskSincePeriod(104880, "201403");
        System.out.println(lst);
        log.info("-----------------End");
    }

}
