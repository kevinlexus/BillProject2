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

    public static void main(String[] args) {
        log.info("");
        log.info("****************************************************************");
        log.info("*                                                              *");
        log.info("*                                                              *");
        log.info("*                    Версия модуля - 1.4.32                    *");
        log.info("*                                                              *");
        log.info("*                                                              *");
        log.info("****************************************************************");
        log.info("");
        log.info("21.01.24 - исправлено REQUIRED вместо REQUIRES_NEW в Builder ГИС");
        log.info("25.01.24 - исправлено REQUIRED вместо REQUIRES_NEW в Builder ГИС - вернул назад");
        log.info("25.01.24 - добавил логирование постановки в очередь заданий ГИС");
        log.info("02.03.24 - добавил логирование пени. Доработал загрузку ПД в ГИС");
        log.info("");
        log.info("****************************************************************");

        SpringApplication.run(BillDirectApplication.class, args);
    }


}
