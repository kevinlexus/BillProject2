import com.dic.app.config.Config;
import com.dic.app.gis.service.maintaners.impl.UlistMng;
import com.dic.app.service.impl.ChangeMngImpl;
import com.dic.bill.dao.*;
import com.dic.bill.mm.ObjParMng;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiRef;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestDAO {


    @Autowired
    private DebDAO debUslDao;
    @Autowired
    private ChangeMngImpl changeMng;
    @Autowired
    private VchangeDetDAO vchangeDetDao;
    @Autowired
    private KwtpDayDAO kwtpDayDao;
    @Autowired
    private CorrectPayDAO correctPayDao;
    @Autowired
    private SaldoUslDAO saldoUslDao;
    @Autowired
    private UlistMng ulistMng;
    @Autowired
    private ObjParMng objParMng;

    @Test
    public void mainWork() {
        log.info("Test Start!");
/*
		log.info("Test Debit");
		debUslDao.getDebitByLsk("00000084", 201403).forEach(t-> {
			log.info("Debit: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test Charge");
		chargeDao.getChargeByLsk("00000084").forEach(t-> {
			log.info("Charge: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test VchangeDet");
		vchangeDetDao.getVchangeDetByLsk("00000084").forEach(t-> {
			log.info("Change: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test KwtpDay оплата долга");
		kwtpDayDao.getKwtpDaySumByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=1: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test KwtpDay оплата пени");
		kwtpDayDao.getKwtpDayPenByLsk("00000084").forEach(t-> {
			log.info("KwtpDay tp=0: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});

		log.info("Test CorrectPay");
		correctPayDao.getCorrectPayByLsk("00000084").forEach(t-> {
			log.info("CorrectPay: usl={}, org={}, summa={}, mg={}, dt={}, tp={}",
					t.getUslId(), t.getOrgId(), t.getDebOut(), t.getMg(), t.getTs(), t.getTp());
		});
*/
        log.info("Test VchargePay");
        saldoUslDao.getVchargePayByLsk("00000085", 201403).forEach(t -> {
            log.info("VchargePay: mg={}, summa={}", t.getMg(), t.getSumma());
        });

        log.info("Test ChargeNabor");
        saldoUslDao.getChargeNaborByLsk("00000085", 201403).forEach(t -> {
            log.info("ChargeNabor: uslId={}, orgId={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        });

        log.info("Test SaldoUsl");
        saldoUslDao.getSaldoUslByLsk("00000085", "201403").forEach(t -> {
            log.info("SaldoUsl: uslId={}, orgId={}, summa={}", t.getUslId(), t.getOrgId(), t.getSumma());
        });
        log.info("Test End!");
    }


    @Test
    public void checkTransactional() throws WrongGetMethod, WrongParam {
        log.info("check 1");
        int klskId = 104735;
        //Boolean isBillAlreadySended = objParMng.getBool(klskId, "bill_sended_via_email");
        //log.info("check 2={}", isBillAlreadySended);

        objParMng.setBoolNewTransaction(klskId, "bill_sended_via_email", true);
        Boolean isBillAlreadySended = objParMng.getBool(klskId, "bill_sended_via_email");
        log.info("check 3={}", isBillAlreadySended);
        Assertions.assertTrue(isBillAlreadySended);

        objParMng.setBoolNewTransaction(klskId, "bill_sended_via_email", false);
        isBillAlreadySended = objParMng.getBool(klskId, "bill_sended_via_email");
        log.info("check 4={}", isBillAlreadySended);
        Assertions.assertFalse(isBillAlreadySended);
    }

    @Test
    public void checkGisNsi() {
        NsiRef mres = ulistMng.getResourceByUsl("011");
        log.info("011 mres.code={}, mres.name={}", mres.getCode(), mres.getName());
        mres = ulistMng.getResourceByUsl("015");
        log.info("015 mres.code={}, mres.name={}", mres.getCode(), mres.getName());
        mres = ulistMng.getResourceByUsl("024");
        log.info("024 mres.code={}, mres.name={}", mres.getCode(), mres.getName());
    }

    @Test
    public void checkChangeDAO() {
        changeMng.getChangeById(194334);

    }
}
