import com.dic.app.Config;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.LskCharge;
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

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static java.util.stream.Collectors.groupingBy;

/**
 * Исправный модуль, для тестирования Spring beans
 *
 * @author lev
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestAchargeDAO {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private AchargeDAO achargeDao;
    @Autowired
    private KartDAO kartDAO;

    /**
     * Проверить, работают ли запросы
     *
     * @throws Exception
     */
    @Test
    @Rollback(false)
    public void isWorkAFlowDAOgetChrgGrp() throws Exception {

        log.info("-----------------Begin");
        achargeDao.getChrgGrp("00000049", 201404, 706813).forEach(t -> {
            log.info("Check Ulist.id={}, chrg={}, chng={}, vol={}, price={}",
                    t.getUlistId(), t.getChrg(), t.getChng(), t.getVol(), t.getPrice());
        });
        log.info("-----------------End");
    }


    /**
     * Проверка projection для перерасчетов
     */
    @Test
    public void checkProjectionForChanges() {
        log.info("-----------------Begin");
        // Map устроен: klskId, (mg, (uslId, List<LskCharges>))
        Map<Long, Map<String, Map<String, List<LskCharge>>>> charges;

        List<LskCharge> lst = kartDAO.getArchChargesByKulNd(0, 1, 0, 0, 0, 2,
                Arrays.asList("201402", "201403", "201404"), "0001", "000033",
                Arrays.asList("003", "005", "011", "013", "015", "029", "033", "038")
        );
        charges = lst.stream().collect(groupingBy(LskCharge::getKlskId, groupingBy(LskCharge::getMg, groupingBy(LskCharge::getUslId))));

        charges.entrySet().stream().limit(100).forEach(t -> {
                    if (t.getKey().equals(105485L)) {
                        log.info("klskId={}", t.getKey());
                        t.getValue().forEach((key, value) -> {
                            log.info("  usl={}", key);
                            value.forEach((key2, value2) -> {
                                log.info("   mg={}", key2);
                                value2.forEach(e -> {
                                    log.info("    lsk={}, org={}, summa={}, vol={}",
                                            e.getLsk(), e.getOrgId(), e.getSumma(), e.getVol());
                                });
                            });
                        });
                    }
                }
        );
        log.info("-----------------End");
    }

}
