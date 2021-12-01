package com.dic.app;

import com.dic.app.gis.service.soap.impl.SoapConfig;
import com.dic.app.gis.sign.commands.SignCommand;
import com.dic.app.gis.sign.commands.SignCommands;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;


@SpringBootApplication
@Slf4j
public class BillDirectApplication {

    public static SignCommands sc;
    public static SignCommands sc2;

    public static void main(String[] args) {
        ConfigurableApplicationContext app = SpringApplication.run(BillDirectApplication.class, args);

        log.info("");
        log.info("****************************************************************");
        log.info("*                                                              *");
        log.info("*                                                              *");
        log.info("*                    Версия модуля - 1.3.10                     *");
        log.info("*                                                              *");
        log.info("*                                                              *");
        log.info("****************************************************************");
        log.info("");

        SoapConfig soapConfig = app.getBean(SoapConfig.class);
        //Создать первый объект подписывания XML
        try {
            sc = buildSigner(soapConfig, 1);
            log.info("Объект подписывания XML-1 СОЗДАН!");
        } catch (Exception e1) {
            log.error("****************************************************************");
            log.error("*                                                              *");
            log.error("*                                                              *");
            log.error("* Объект подписывания XML-1 не создан, приложение ОСТАНОВЛЕНО! *");
            log.error("*                                                              *");
            log.error("*                                                              *");
            log.error("****************************************************************");
            log.error("stackTrace={}", Utl.getStackTraceString(e1));
            // Завершить выполнение приложения
            SpringApplication.exit(app, () -> 0);
        }

        //Создать второй объект подписывания XML (при наличии)
        if (soapConfig.getSignPass2() != null) {
            try {
                sc2 = buildSigner(soapConfig, 2);
                log.info("Объект подписывания XML-2 СОЗДАН!");
            } catch (Exception e1) {
                log.error("****************************************************************");
                log.error("*                                                              *");
                log.error("*                                                              *");
                log.error("* Объект подписывания XML-2 не создан, приложение ОСТАНОВЛЕНО! *");
                log.error("*                                                              *");
                log.error("*                                                              *");
                log.error("****************************************************************");
                log.error("stackTrace={}", Utl.getStackTraceString(e1));
                // Завершить выполнение приложения
                SpringApplication.exit(app, () -> 0);
            }
        }



    }


    /**
     * Создать объект подписывания
     *
     * @param soapConfig - конфиг
     * @param cnt        - номер объекта по порядку
     * @return объект подписывания
     */
    private static SignCommands buildSigner(SoapConfig soapConfig, int cnt) throws Exception {
        if (cnt == 1) {
            // первый объект
            if (soapConfig.getSignPath() == null) {
                throw new RuntimeException("Не установлен параметр signPath в application.properties!");
            }
            return new SignCommand(soapConfig.getSignPass(), soapConfig.getSignPath());
        } else {
            // второй объект
            if (soapConfig.getSignPath2() == null) {
                throw new RuntimeException("Не установлен параметр signPath2 в application.properties!");
            }
            return new SignCommand(soapConfig.getSignPass2(), soapConfig.getSignPath2());
        }
    }

}
