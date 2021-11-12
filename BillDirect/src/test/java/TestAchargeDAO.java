import com.dic.app.Config;
import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskChargeUsl;
import com.dic.bill.dto.Selobj;
import com.dic.bill.enums.SelObjTypes;
import com.dic.bill.model.scott.Ko;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
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
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

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
    public void checkProjectionForChanges() throws JsonProcessingException, ExecutionException, InterruptedException, ErrorWhileGen, WrongParam {
        log.info("-----------------Begin");


        ObjectMapper objectMapper = new ObjectMapper();
        //ChangesParam changesParam = objectMapper.readValue("{\"periodFrom\": \"201401\",\"periodTo\": \"201404\",\"selObjList\":[{\"id\":\"54\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"104887\",\"tp\":\"1\"}],\"isAddUslSvSocn\": \"true\",\"isAddUslWaste\": \"true\",\"processMeter\": \"1\",\"processAccount\": \"0\",\"processStatus\": \"2\",\"processLskTp\": \"2\",\"processTp\": \"1\",\"processEmpty\": \"0\",\"comment\": \"коммент\",\"changeUslList\": [{\"uslId\":\"011\",\"org1Id\":\"4\",\"proc1\":\"23\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"015\",\"org1Id\":\"5\",\"proc1\":\"15\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"}]}", ChangesParam.class);
        ChangesParam changesParam = objectMapper.readValue("{\"periodFrom\": \"201401\",\"periodTo\": \"201404\",\"selObjList\":[" +
                "{\"id\":\"1\",\"kul\":\"0001\",\"nd\":\"00017А\",\"tp\":\"0\"}," +
                "{\"id\":\"2\",\"klskId\":\"104880\",\"tp\":\"1\"}," +
                "{\"id\":\"3\",\"lsk\":\"00000010\",\"tp\":\"2\"}]," +
                "\"isAddUslSvSocn\": \"true\",\"isAddUslWaste\": \"true\",\"processMeter\": \"1\",\"processAccount\": \"0\",\"processStatus\": \"2\",\"processLskTp\": \"2\",\"processTp\": \"1\",\"processEmpty\": \"0\",\"comment\": \"коммент\",\"changeUslList\": [{\"uslId\":\"011\",\"org1Id\":\"4\",\"proc1\":\"23\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"},{\"uslId\":\"015\",\"org1Id\":\"5\",\"proc1\":\"15\",\"org2Id\":\"\",\"proc2\":\"\",\"absSet\":\"\",\"cntDays\":\"\",\"cntDays2\":\"\"}]}", ChangesParam.class);
        processMng.processChanges(changesParam);

        log.info("-----------------End");
    }

}
