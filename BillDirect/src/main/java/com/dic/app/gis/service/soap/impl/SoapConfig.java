package com.dic.app.gis.service.soap.impl;

import com.dic.app.gis.service.soap.SoapConfigs;
import com.dic.app.gis.sign.commands.SignCommand;
import com.dic.app.gis.sign.commands.SignCommands;
import com.dic.bill.dao.UserDAO;
import com.dic.bill.model.exs.Eolink;
import com.ric.cmn.CommonUtl;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.UnusableCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;

@Slf4j
@Service
@RequiredArgsConstructor
public class SoapConfig implements SoapConfigs {


    private final UserDAO userDao;
    @Value("${baseOrgGUID}")
    private String baseOrgGUID;

    @Value("${signPass}")
    private String signPass;
    @Value("${signPath:#{null}}")
    private String signPath;

    @Value("${signPass2:#{null}}")
    private String signPass2;
    @Value("${signPath2:#{null}}")
    private String signPath2;

    // вхождение (для ГОСТ 2012)
    @Value("${signEntry:#{null}}")
    private String signEntry;
    @Value("${signEntry2:#{null}}")
    private String signEntry2;

    @Value("${hostIp}")
    private String hostIp;

    @Value("${parameters.gis.sign.enabled}")
    private Boolean signEnabled;

    private final ApplicationContext ctx;
    public static SignCommands sc;
    public static SignCommands sc2;
    @Getter
    private boolean isGisKeysLoaded;

    /**
     * Получить OrgPPGUID организации
     */
    @Override
    public String getOrgPPGuid() {
        // Базовый GUID организации, осуществляющей обмен! (для справочников NSI и т.п.)
        return baseOrgGUID;
    }

    /**
     * Получить URL endpoint
     */
    @Override
    public String getHostIp() {
        return hostIp;
    }

    /**
     * Вернуть префикс CD элементов в справочниках локальной системы
     */
    @Override
    public String getPrefixGis() {
        return "GIS";
    }

    /*
     */
/**
 * Вернуть пользователя, от имени которого выполняются процессы
 *//*

    @Override
    public User getCurUser() {
        return this.user;
    }
*/

    /**
     * Получить объект уровня РКЦ - по объекту типа Дом
     *
     * @param house - объект Eolink - дом
     */
    @Override
    public Eolink getRkcByHouse(Eolink house) {
        Eolink uk = house.getParent();
        return uk.getParent();
    }

    @Override
    public String getSignPass() {
        return signPass;
    }

    @Override
    public String getSignPath() {
        return signPath;
    }

    @Override
    public String getSignPass2() {
        return signPass2;
    }

    @Override
    public String getSignPath2() {
        return signPath2;
    }

    public String getSignEntry() {
        return signEntry;
    }

    public String getSignEntry2() {
        return signEntry2;
    }

    /**
     * Сохранить ошибки
     *
     * @param eolink - объект
     * @param mask   - битовый код маски
     */
    @Override
    public void saveError(Eolink eolink, long mask, boolean isSet) throws UnusableCode {
        long errPrev = 0L, errActual;
        if (eolink.getErr() != null) {
            errPrev = eolink.getErr();
        }
        if (isSet) {
            // совместить биты ошибки с источником
            errActual = errPrev | mask;
        } else {
            // обнулить соответствующие биты ошибки
            errActual = errPrev & (~mask);
        }
        if (errPrev != errActual) {
            // сохранить в случае изменения значения
            String comm = CommonUtl.getErrorDescrByCode(errActual);
            eolink.setComm(comm);
            eolink.setErr(errActual);
        }
    }

    @PostConstruct
    public void init() {
        if (signEnabled) {
            SoapConfig soapConfig = ctx.getBean(SoapConfig.class);
            //Создать первый объект подписывания XML
            try {
                sc = buildSigner(soapConfig, 1);
                log.info("Объект подписывания XML-1 СОЗДАН!");
                isGisKeysLoaded = true;
            } catch (Exception e1) {
                isGisKeysLoaded = false;
                log.error("********************************************************************");
                log.error("*                                                                  *");
                log.error("*                                                                  *");
                log.error("* Объект подписывания XML-1 не создан, выполнение ГИС ОСТАНОВЛЕНО! *");
                log.error("*                                                                  *");
                log.error("*                                                                  *");
                log.error("********************************************************************");
                log.error("stackTrace={}", Utl.getStackTraceString(e1));
            }

            //Создать второй объект подписывания XML (при наличии)
            if (soapConfig.getSignPass2() != null) {
                try {
                    sc2 = buildSigner(soapConfig, 2);
                    log.info("Объект подписывания XML-2 СОЗДАН!");
                } catch (Exception e1) {
                    isGisKeysLoaded = false;
                    log.error("********************************************************************");
                    log.error("*                                                                  *");
                    log.error("*                                                                  *");
                    log.error("* Объект подписывания XML-2 не создан, выполнение ГИС ОСТАНОВЛЕНО! *");
                    log.error("*                                                                  *");
                    log.error("*                                                                  *");
                    log.error("********************************************************************");
                    log.error("stackTrace={}", Utl.getStackTraceString(e1));
                }
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
