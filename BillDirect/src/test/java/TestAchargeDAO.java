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
    @Rollback(value = false)
    public void checkProjectionForChanges() throws JsonProcessingException, ExecutionException, InterruptedException, ErrorWhileGen, WrongParam {
        log.info("-----------------Begin");


        ObjectMapper objectMapper = new ObjectMapper();
        ChangesParam changesParam = objectMapper.readValue("{\"dt\": \"17.04.2014\",\"user\": \"bugh1\",\"periodFrom\": \"201401\",\"periodTo\": \"201404\",\"periodProcess\": \"201401\",\"selObjList\":[{\"id\":\"31\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"\",\"tp\":\"0\"}, {\"id\":\"225\",\"kul\":\"0001\",\"nd\":\"000033\",\"klskId\":\"\",\"tp\":\"0\"}, {\"id\":\"42\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"559766\",\"tp\":\"1\"}, {\"id\":\"40\",\"kul\":\"0001\",\"nd\":\"00017А\",\"klskId\":\"559762\",\"tp\":\"1\"}],\"isAddUslSvSocn\": \"true\",\"isAddUslWaste\": \"true\",\"processMeter\": \"1\",\"processAccount\": \"0\",\"processStatus\": \"2\",\"processLskTp\": \"2\",\"processTp\": \"1\",\"processEmpty\": \"0\",\"comment\": \"коммент\",\"changeUslList\": [{\"uslId\":\"003\",\"orgId\":\"\",\"proc\":\"15\",\"absSet\":\"\",\"cntDays\":\"11\"},{\"uslId\":\"011\",\"orgId\":\"5\",\"proc\":\"26\",\"absSet\":\"\",\"cntDays\":\"\"}]}", ChangesParam.class);
        processMng.processChanges(changesParam);

        log.info("-----------------End");
    }

}
