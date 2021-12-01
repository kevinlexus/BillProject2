package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.EolinkMng;
import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.Org;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.WrongGetMethod;
import lombok.extern.slf4j.Slf4j;

import java.util.List;

/**
 * Хранилище настроек SOAP запроса
 *
 * @author lev
 */
@Slf4j
public class ReqProp {

    //    private Task foundTask;
    private String houseGuid;
    private String ppGuid;
    private String kul;
    private String nd;
    // УК по данному Task в виде объекта EOLINK
    private Eolink eolinkUk;
    // УК по данному Task в виде объекта T_ORG
    private Org uk;
    // Id подписчика XML (если hostIp не заполнен, то определяется по УК)
    private int signerId = 1;
    private String hostIp;
    private String gisVersion;

    /*
     * Установить значения настроек до создания объекта SoapBuilder
     */
    public ReqProp(ConfigApp config, Task task, EolinkParMng eolParMng) throws CantPrepSoap {
        Eolink eolink = task.getEolink();
        this.uk = config.getMapReuOrg().get(eolink.getOrg().getReu()); // ред.08.08.2019		this.reu = eolink.getUk().getReu(); // ред.07.08.2019
        kul = eolink.getKul();
        nd = eolink.getNd();
        houseGuid = eolink.getGuid();

        // получить УК
        if (task.getProcUk() == null) {
            eolinkUk = getUkByTaskEolink(eolink, task);
        } else {
            eolinkUk = task.getProcUk();
        }

        ppGuid = eolinkUk.getGuid();
        if (ppGuid == null) {
            if (eolinkUk.getParent().getGuid() == null) {
                throw new CantPrepSoap("Не заполнен GUID организации EOLINK.ID=" + eolinkUk.getId());
            } else {
                // получить PPGUID у родительской организации (например 112 УК получает у 063 в Кис.) // ред.24.08.21
                ppGuid = eolinkUk.getParent().getGuid();
            }
        }
        // IP адрес сервиса STUNNEL, получить или из application.properties - hostIp (Кис, Полыс)
        // или из параметра по УК (ТСЖ Содружество, Свободы)
        gisVersion = config.getGisVersion();
        hostIp = config.getHostIp();
        if (hostIp == null) {
            try {
                hostIp = eolParMng.getStr(eolinkUk, "ГИС ЖКХ.HOST_IP");
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
                throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.HOST_IP' по организации Eolink.id=" + eolinkUk.getId());
            }
            try {
                Double signerIdD = eolParMng.getDbl(eolinkUk, "ГИС ЖКХ.SIGNER_ID");
                signerId = signerIdD.intValue();
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
                throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.SIGNER_ID' по организации Eolink.id=" + eolinkUk.getId());
            }
        }
        if (hostIp == null) {
            throw new CantPrepSoap("Не заполнен параметр hostIp по организации Eolink.id=" + eolinkUk.getId()
                    + "(ТСЖ Свобод) либо не заполнен application.properties - hostIp (Кис.Полыс.)");
        }
    }

    /*
     * Установить значения настроек до создания объекта SoapBuilder для простых запросов,
     * типа получения параметров организации
     *
     */
    public ReqProp(ConfigApp config, Task task, EolinkParMng eolParMng, int kmp) throws CantPrepSoap {
        // получить GUID организации с уровня РКЦ
        Eolink uk = task.getEolink();
        ppGuid = uk.getParent().getGuid();

        // IP адрес сервиса STUNNEL, получить или из application.properties - hostIp (Кис, Полыс)
        // или из параметра по УК (ТСЖ Содружество, Свободы)
        gisVersion = config.getGisVersion();
        hostIp = config.getHostIp();
        if (hostIp == null) {
            try {
                hostIp = eolParMng.getStr(uk, "ГИС ЖКХ.HOST_IP");
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
                throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.HOST_IP' по организации Eolink.id=" + uk.getId());
            }
        }
        if (hostIp == null) {
            throw new CantPrepSoap("Не заполнен параметр hostIp по организации Eolink.id=" + uk.getId()
                    + "(ТСЖ Свобод) либо не заполнен application.properties - hostIp (Кис.Полыс.)");
        }
        try {
            Double signerIdD = eolParMng.getDbl(uk, "ГИС ЖКХ.SIGNER_ID");
            signerId = signerIdD.intValue();
        } catch (WrongGetMethod wrongGetMethod) {
            wrongGetMethod.printStackTrace();
            throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.SIGNER_ID' по организации Eolink.id=" + uk.getId());
        }
    }

    /*
     * Установить значения настроек до создания объекта SoapBuilder для еще более простых запросов
     *
     */
    public ReqProp(ConfigApp config, EolinkMng eolinkMng, EolinkParMng eolParMng) throws CantPrepSoap {
        // IP адрес сервиса STUNNEL, получить или из application.properties - hostIp (Кис, Полыс)
        // или из параметра по УК (ТСЖ Содружество, Свободы)
        gisVersion = config.getGisVersion();
        hostIp = config.getHostIp();
        if (hostIp == null) {
            // получить GUID организации с уровня РКЦ
            List<Eolink> ukList = eolinkMng.getEolinkUk();
            eolinkUk = ukList.get(0);
            ppGuid = eolinkUk.getGuid();
            try {
                hostIp = eolParMng.getStr(eolinkUk, "ГИС ЖКХ.HOST_IP");
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
                throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.HOST_IP' по организации Eolink.id=" + uk.getId());
            }
            if (hostIp == null) {
                throw new CantPrepSoap("Не заполнен параметр hostIp по организации Eolink.id=" + uk.getId()
                        + "(ТСЖ Свобод) либо не заполнен application.properties - hostIp (Кис.Полыс.)");
            }
            try {
                Double signerIdD = eolParMng.getDbl(eolinkUk, "ГИС ЖКХ.SIGNER_ID");
                signerId = signerIdD.intValue();
            } catch (WrongGetMethod wrongGetMethod) {
                wrongGetMethod.printStackTrace();
                throw new CantPrepSoap("Ошибка при получении параметра 'ГИС ЖКХ.SIGNER_ID' по организации Eolink.id=" + uk.getId());
            }
        }
    }


    /*
     * Установить значения настроек после создания объекта SoapBuilder
     */
/*

    public void setPropAfter(Task task) {
        // найти Task, для использования в транзации
        foundTask = em.find(Task.class, task.getId());
    }
*/

    /*
     * Установить значения настроек после создания объекта SoapBuilder
     */
/*
	@Override
	@Transactional(readOnly = false, propagation = Propagation.REQUIRED, rollbackFor=Exception.class)
	public void setPropAfter(Task task, SoapBuilder sb) throws CantPrepSoap {
		this.sb = sb;
		// GUID организации от которой выполняется запрос
		sb.setPpGuid(ppGuid);
		// IP адрес сервиса STUNNEL
		//log.info("Использованный hostIp2={}", hostIp);
		//sb.setHostIp(hostIp);
	}
*/

    /*
     * Установить значения настроек
     */

/*
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void setPropWOGUID(Task task, SoapBuilder sb) {
        if (task.getEolink() != null) {
            reu = task.getEolink().getOrg().getReu();
            kul = task.getEolink().getKul();
            nd = task.getEolink().getNd();
            houseGuid = task.getEolink().getGuid();
        }
        // GUID текущей организации
        this.ppGuid = config.getOrgPPGuid();
    }
*/

    /**
     * Получить рекурсивно eolink УК
     *
     * @param eolink текущий объект
     * @param task   задание
     */
    private Eolink getUkByTaskEolink(Eolink eolink, Task task) throws CantPrepSoap {
        Eolink eolFound;
        if (eolink.getObjTp().getCd().equals("Организация")) {
            eolFound = eolink;
        } else {
            eolFound = getUkByTaskEolink(eolink.getParent(), task);
        }
        return eolFound;

    }


    public String getHouseGuid() {
        return houseGuid;
    }


    public String getKul() {
        return kul;
    }


    public String getNd() {
        return nd;
    }


    public Org getUk() {
        return uk;
    }

    public String getGisVersion() {
        return gisVersion;
    }


    public String getPpGuid() {
        return ppGuid;
    }


    public String getHostIp() {
        return hostIp;
    }


    public int getSignerId() {
        return signerId;
    }
}
