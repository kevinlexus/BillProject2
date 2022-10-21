import com.dic.app.config.Config;
import com.dic.app.service.registry.RegistryMngImpl;
import com.dic.app.telegram.bot.service.message.MessageStore;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.SumFinanceFlow;
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
import java.io.File;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Тестирование сервиса MeterMng
 *
 * @author lev
 */
@RunWith(SpringRunner.class)
//@ContextConfiguration(classes=Config.class)
//@AutoConfigureTestDatabase(replace = Replace.NONE)
//@DataJpaTest
@Slf4j
public class TestRegistryMng {

/*
	@PersistenceContext
	private EntityManager em;
	@Autowired
	private RegistryMngImpl registryMng;

	@Test
	public void checkFlow() {
		StringBuilder str = registryMng.getFlowFormatted(104880L, "201309");
		log.info(str.toString());
	}
*/

	@Test
	public void renderCharge() {
		//StringBuilder str = registryMng.getChargeFormatted(104880L);
		StringBuilder str = new StringBuilder("""
				Начисление
				|               Услуга| Объем|Цена,руб.|Ед.изм.|Cумма,руб.|
				|           Cодер/c.н.|  66.1|       26|     м2|   1,718.6|
				| Вывоз мус.быт.2/c.н.|  1.83|     1.85|   чел.|      3.39|
				|      Польз.лифт/c.н.|  66.1|     3.77|     м2|     249.2|
				|     Дератизация/c.н.|  66.1|     0.13|     м2|      8.59|
				|           Отопл/c.н.| 34.58|    14.28|   гкал|    493.85|
				|     Отопл./ 0 зарег.|  6.92|    28.13|   гкал|    194.57|
				|      Кап ремонт/c.н.| 55.08|      3.2|     м2|    176.26|
				|     Кап ремонт/св.н.| 11.02|      3.2|     м2|     35.26|
				|        Холодная вода|   7.4|     20.6|  куб.м|    152.44|
				|Холодн.вода/ 0 зарег.|  1.48|    28.54|  куб.м|     42.24|
				|           Горяч.вода|  6.85|    25.49|  куб.м|    174.61|
				|     Горяч.вода/св.н.|     3|    43.96|  куб.м|    131.85|
				| Горяч.вода/ 0 зарег.|  1.97|     84.7|  куб.м|    166.84|
				|        Водоотведение|    17|    12.39|  куб.м|    210.63|
				|  Водоотведение/св.н.|   3.7|    12.39|  куб.м|     45.83|
				|            Код.зам-2|     1|       36|   точ.|        36|
				|  Эл.энерг.2/соц.нор.|166.67|     1.85|    квт|    308.33|
				|     Эл.энерг.2/св.н.| 21.67|     1.85|    квт|     40.08|
				|            Антенна-1|     1|      145|   точ.|       145|
				|        Электроэнерг7|166.67|        1|    квт|    166.67|
				""");
		Utl.renderImage(str, "Lucida Console", 14, 25);
		log.info(str.toString());
	}


}