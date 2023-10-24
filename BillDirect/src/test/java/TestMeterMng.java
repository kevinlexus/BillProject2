import com.dic.app.config.Config;
import com.dic.app.gis.service.soapbuilders.impl.DeviceMeteringAsyncBindingBuilder;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Meter;
import com.ric.cmn.Utl;
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
import javax.xml.datatype.XMLGregorianCalendar;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.*;

import static org.junit.Assert.assertTrue;

/**
 * Тестирование сервиса MeterMng
 *
 * @author lev
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestMeterMng {

	@PersistenceContext
	private EntityManager em;
	@Autowired
	private MeterDAO meterDao;
	@Autowired
	private MeterMng meterMng;
	@Autowired
	private DeviceMeteringAsyncBindingBuilder deviceMeteringAsyncBindingBuilder;

	/**
	 * Тест на наличие показаний счетчика по GUID и Timestamp
	 *
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isFindMeterDataByGuidTs() throws Exception {
		String guid = "2312-1316-4564-4654-4463";
		class LocalMeterData implements MeterData {
			public Date getTs() {
				try {
					return Utl.getDateFromStr("30.03.2014");
				} catch (ParseException e) {
					e.printStackTrace();
				}
				return null;
			}

			public String getGuid() {
				return guid;
			}
		}

		int i = 0, i2 = 0;
		log.info("-----------------Begin");
		// найти счетчики х.в. по объекту Ko
		XMLGregorianCalendar ts = Utl.getXMLDate(Utl.getDateFromStr("30.03.2014"));
		Map<String, Date> existVal = new HashMap<>();
		existVal.put(guid, Utl.getDateFromXmlGregCal(ts));

		assertTrue(meterMng.getIsMeterDataExist(existVal, guid, ts));

		log.info("-----------------End");
	}

	/**
	 * Проверка OneToOne
	 *
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void justTest() throws Exception {
		Ko ko = em.find(Ko.class, 524877);
		Eolink eolink = ko.getEolink();
		log.info("Eolink={}", eolink.getId());

		Eolink e2 = em.find(Eolink.class, 707565);
		Ko ko2 = e2.getKoObj();
		log.info("Ko={}", ko2.getId());

		Meter meter = em.find(Meter.class, 73522);
		Ko ko3 = meter.getKo();
		Ko ko4 = meter.getKoObj();
		log.info("Ko3={}, Ko4={}", ko3.getId(), ko4.getId());

		Kart kart = em.find(Kart.class, "00000239");

		log.info("Ko5={}, Ko6={}", kart.getKoKw().getId(), kart.getKoLsk().getId());
	}

	/**
	 * Проверка MeterMng.getIsMeterActual
	 * @throws Exception
	 */
	@Test
	@Rollback(true)
	public void isWorkingGetIsMeterActual() throws Exception
	{
		Eolink meterEol = em.find(Eolink.class, 708094);
		Ko ko = meterEol.getKoObj();
		Meter meter = ko.getMeter();
		log.info("actual dt1={}, dt2={}", meter.getDt1(), meter.getDt2());
	}

	@Test
	@Rollback(true)
	public void isWorkingsaveMeterData() throws Exception
	{
		Eolink meterEol = em.find(Eolink.class, 708441);
		XMLGregorianCalendar ts = Utl.getXMLDate(Date.from(LocalDate.of(2014, 4, 15)
				.atStartOfDay(ZoneId.systemDefault()).toInstant()));
		deviceMeteringAsyncBindingBuilder.saveMeterData(meterEol, "325.25", ts, DeviceMeteringAsyncBindingBuilder.STATUS_LOADED_FROM_GIS);
	}


}