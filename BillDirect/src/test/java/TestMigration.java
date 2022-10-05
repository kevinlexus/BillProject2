import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase;
import org.springframework.boot.test.autoconfigure.jdbc.AutoConfigureTestDatabase.Replace;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import com.dic.app.config.Config;
import com.dic.app.service.ConfigApp;
import com.dic.app.service.MigrateMng;

import lombok.extern.slf4j.Slf4j;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes=Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestMigration {


	@PersistenceContext
    private EntityManager em;
	@Autowired
    private MigrateMng migrateMng;
	@Autowired
    private ConfigApp config;

/*
    @Test
    @Rollback(false)
    public void mainTestMigration() throws ErrorWhileDistDeb {
		log.info("Test start, period={}", config.getPeriod());

		//migrateMng.migrateAll("00000086", "00000086", 1);

		log.info("Test end");
	}
*/


}
