import com.dic.app.Config;
import com.dic.app.mm.ChangeMng;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskCharge;
import com.dic.bill.dto.Selobj;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.concurrent.ExecutionException;

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
    @Autowired
    private ProcessMng processMng;

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
    public void checkProjectionForChanges() throws JsonProcessingException, ExecutionException, InterruptedException {
        log.info("-----------------Begin");
        // Map устроен: klskId, (mg, (uslId, List<LskCharges>))

/*
        Selobj selObj = new Selobj(1, "0001", "000033", null, 0);
        ChangesParam changesParam = ChangesParam.builder().isAddUslKan(false).isAddUslSvSocn(false)
                .comment("комментарий").isProcessEmpty(true).processLskTp(2).processMeter(1).changeUslList(changeUslList);
*/
        ObjectMapper objectMapper = new ObjectMapper();
        //ChangesParam changesParam = objectMapper.readValue("{\"periodFrom\": \"201401\",\"periodTo\": \"201404\",\"periodProcess\": \"200901\",\"selObjList\":[{\"id\":\"31\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"\",\"tp\":\"0\"}, {\"id\":\"225\",\"kul\":\"0001\",\"nd\":\"000033\",\"klskId\":\"\",\"tp\":\"0\"}, {\"id\":\"10\",\"kul\":\"0174\",\"nd\":\"000012\",\"klskId\":\"559590\",\"tp\":\"1\"}, {\"id\":\"9\",\"kul\":\"0174\",\"nd\":\"000012\",\"klskId\":\"559596\",\"tp\":\"1\"}, {\"id\":\"8\",\"kul\":\"0174\",\"nd\":\"000012\",\"klskId\":\"559600\",\"tp\":\"1\"}],\"isAddUslSvSocn\": \"true\",\"isAddUslWaste\": \"true\",\"processMeter\": \"1\",\"processAccount\": \"0\",\"processStatus\": \"2\",\"processLskTp\": \"2\",\"processTp\": \"1\",\"processEmpty\": \"0\",\"comment\": \"коммент\",\"changeUslList\": [{\"uslId\":\"003\",\"org1Id\":\"2\",\"proc1\":\"10\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"011\",\"org1Id\":\"3\",\"proc1\":\"12\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"040\",\"org1Id\":\"5\",\"proc1\":\"15\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"041\",\"org1Id\":\"6\",\"proc1\":\"16\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"005\",\"org1Id\":\"6\",\"proc1\":\"18\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"006\",\"org1Id\":\"7\",\"proc1\":\"1.2\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"007\",\"org1Id\":\"8\",\"proc1\":\"1.25\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"008\",\"org1Id\":\"8\",\"proc1\":\"10\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"061\",\"org1Id\":\"8\",\"proc1\":\"19\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"}]}", ChangesParam.class);
        ChangesParam changesParam = objectMapper.readValue("{\"periodFrom\": \"201401\",\"periodTo\": \"201404\",\"selObjList\":[{\"id\":\"54\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"104887\",\"tp\":\"1\"}],\"isAddUslSvSocn\": \"true\",\"isAddUslWaste\": \"true\",\"processMeter\": \"1\",\"processAccount\": \"0\",\"processStatus\": \"2\",\"processLskTp\": \"2\",\"processTp\": \"1\",\"processEmpty\": \"0\",\"comment\": \"коммент\",\"changeUslList\": [{\"uslId\":\"011\",\"org1Id\":\"4\",\"proc1\":\"23\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"015\",\"org1Id\":\"5\",\"proc1\":\"15\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"}]}", ChangesParam.class);
        processMng.processChanges(changesParam);

        log.info("-----------------End");
    }

}
