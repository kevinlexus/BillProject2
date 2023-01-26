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
        log.info("*                    Версия модуля - 1.4.23                    *");
        log.info("*                                                              *");
        log.info("*                                                              *");
        log.info("****************************************************************");
        log.info("");

        SpringApplication.run(BillDirectApplication.class, args);
    }


}
