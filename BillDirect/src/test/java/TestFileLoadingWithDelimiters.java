import com.dic.app.config.Config;
import com.dic.app.service.registry.RegistryMngImpl;
import com.dic.bill.dao.OrgDAO;
import com.dic.bill.dto.UnloadPaymentParameter;
import com.dic.bill.model.scott.Org;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileLoad;
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
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Config.class)
@AutoConfigureTestDatabase(replace = Replace.NONE)
@DataJpaTest
@Slf4j
public class TestFileLoadingWithDelimiters {


    @Autowired
    private RegistryMngImpl registryMng;
    @Autowired
    private OrgDAO orgDAO;

    @Test
    public void testScanner() {
        Scanner scanner = new Scanner("100416574;dfafc63e-f435-46e9-861f-c1cb2886603c;;г Полысаево,,ул Токарева,дом 2,16;1;Услуга вывоза ТКО;112020;207.30");
        scanner.useDelimiter(";");
        while (scanner.hasNext()) {
            String adr = scanner.next();
            Scanner scannerAdr = new Scanner(adr);
            scannerAdr.useDelimiter(",");
            log.info("adr={}", adr);
            int i = 0;
            while (scannerAdr.hasNext()) {
                String adrPart = scannerAdr.next();
                log.info("index={}, adrPart={}", i++, adrPart);
            }
        }
    }

    /**
     * Загрузить файл с внешними лиц.счетами во временную таблицу
     */
    @Test
    @Rollback(false)
    public void fileLoadKartExt() throws FileNotFoundException, WrongParam, ErrorWhileLoad {
        // загрузить файл во временную таблицу LOAD_KART_EXT
        Org org = orgDAO.getByReu("001");
        //registryMng.loadFileKartExt(org, "d:\\temp\\#1\\Кап\\0206DBSS001012021.txt");
        registryMng.loadFileKartExt(12, "d:\\temp\\#1\\Кап\\0204DBSS001012021.txt");
    }

    @Test
    @Rollback(false)
    public void approveLoadKartExt() throws WrongParam {
        Org org = orgDAO.getByReu("001");
        // загрузить успешно обработанные лиц.счета в таблицу внешних лиц.счетов
        registryMng.loadApprovedKartExt(12);
    }

    /**
     * Выгрузить файл платежей по внешними лиц.счетами
     */
    @Test
    @Rollback(false)
    public void fileUnloadPaymentKartExt() throws IOException, WrongParam {
        registryMng.unloadPaymentFileKartExt(
                new UnloadPaymentParameter(90, Utl.getFirstDt(), Utl.getLastDt(), null, "123"));
    }

    /**
     * Загрузить файл с показаниями по счетчикам
     */
    @Test
    @Rollback(false)
    public void fileLoadMeterVal() throws FileNotFoundException {
        registryMng.loadFileMeterVal("112_102023ИПУ.csv",
                "windows-1251", false);
    }

    @Test
    @Rollback(false)
    public void checkLoadFileSberRegistry() {
        log.info("Итого загружено предварительно:"+registryMng.loadFileSberRegistry("17066_4211017025_40702810426200100859_001", "041"));
    }
    @Test
    @Rollback(false)
    public void checkLoadFileVtbRegistry() {
        log.info("Итого загружено предварительно:"+registryMng.loadFileSberRegistry("201023_VTB_223308_003.txt", "041"));
    }

}
