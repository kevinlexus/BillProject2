import com.dic.app.config.Config;
import com.dic.app.service.ProcessMng;
import com.dic.bill.dao.StatesPrDAO;
import com.dic.bill.mm.KartPrMng;
import com.dic.bill.mm.TestDataBuilder;
import lombok.extern.slf4j.Slf4j;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestKartPrMng {

	@Autowired
	private KartPrMng kartPrMng;
	@Autowired
	private TestDataBuilder testDataBuilder;
	@Autowired
	private ProcessMng processMng;
	@Autowired
	private StatesPrDAO statesPrDao;
	@PersistenceContext
	private EntityManager em;

	/**
	 * Проверка корректности получения кол-ва проживающих
	 */
/*
	@Test
	@Rollback(true)
	@Transactional
	public void checkCountPers() {
		log.info("Test CountPers");

		// загрузить справочники
		CalcStore calcStore = processMng.buildCalcStore(Utl.getDateFromStr("15.04.2014"), 0);
		// построить лиц.счет
		Kart kart = testDataBuilder.buildKartForTest("0000000X", true, true, true);
		em.persist(kart); // note Используй crud.save

		List<StatePr> lstStatesPr = statesPrDao.findByDate(kart.getLsk(),
				calcStore.getCurDt1(), calcStore.getCurDt2());

		Usl usl = em.find(Usl.class, "003");
		CountPers countPers = kartPrMng.getCountPersByDate(kart, 1D, lstStatesPr,
				usl, Utl.getDateFromStr("07.04.2014"));
		log.info("CountPers: kpr={}, kprNorm={}, kprMax={}, kprWr={}, kprOt={} ",
				countPers.kpr, countPers.kprNorm, countPers.kprMax,
				countPers.kprWr, countPers.kprOt);

	}
*/



}
