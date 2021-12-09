package com.dic.app.gis.service.soapbuilders.impl.add;

import com.dic.bill.dao.UserDAO;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.sec.User;
import com.ric.cmn.CommonUtl;
import com.ric.cmn.excp.UnusableCode;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

@Slf4j
@Service("sp")
public class SoapConfig {


    private final UserDAO userDao;
    @Value("${baseOrgGUID}")
    private String baseOrgGUID;
    @Value("${fingerPrint}")
    private String fingerPrint;
    @Value("${basicPass}")
    private String basicPass;
    @Value("${basicLogin}")
    private String basicLogin;

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

    // Пользователь, от имени которого выполняются процессы
    private User user;

    public SoapConfig(UserDAO userDao) {
        this.userDao = userDao;
    }

    // среда выполнения
/*
    public int getEnv() {
        return 0; // HOTORA
    }
*/

    /**
     * Получить OrgPPGUID организации
     */

    public String getOrgPPGuid() {
        // Базовый GUID организации, осуществляющей обмен! (для справочников NSI и т.п.)
        return baseOrgGUID;
    }

    /**
     * Получить URL endpoint
     */

    public String getHostIp() {
        return hostIp;
    }

    /**
     * Получить fingerprint
     */

    public String getFingerPrint() {
        return fingerPrint;
    }

    /**
     * Получить логин basic-авторизации
     */

    public String getBscLogin() {
        return basicLogin;
    }

    /**
     * Получить пароль basic-авторизации
     */

    public String getBscPass() {
        return basicPass;
    }

    /**
     * Вернуть префикс CD элементов в справочниках локальной системы
     */

    public String getPrefixGis() {
        return "GIS";
    }


    public Boolean setUp(Boolean isLoadRef) {
        log.info("загрузка данных пользователя");
        this.user = userDao.getByCd("GEN");
        return true;
    }

    /**
     * Вернуть пользователя, от имени которого выполняются процессы
     */

    public User getCurUser() {
        return this.user;
    }

    /**
     * Получить объект уровня РКЦ - по объекту типа Дом
     *
     * @param house - объект Eolink - дом
     */

    public Eolink getRkcByHouse(Eolink house) {
        Eolink uk = house.getParent();
        return uk.getParent();
    }


    public String getSignPass() {
        return signPass;
    }


    public String getSignPath() {
        return signPath;
    }


    public String getSignPass2() {
        return signPass2;
    }


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

}
