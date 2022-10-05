package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.TaskParMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.maintaners.impl.UlistMng;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.service.ConfigApp;
import com.dic.bill.UlistDAO;
import com.dic.bill.dao.EolinkDAO;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.UlistTp;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType.Error;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.nsi.ExportDataProviderNsiItemRequest;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiElementType;
import ru.gosuslugi.dom.schema.integration.nsi_service_async.NsiPortsTypeAsync;
import ru.gosuslugi.dom.schema.integration.nsi_service_async.NsiServiceAsync;

import javax.persistence.EntityManager;
import javax.xml.ws.BindingProvider;
import java.math.BigInteger;
import java.util.Date;

@Service
@Slf4j
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class NsiServiceAsyncBindingBuilder {


    private final UlistDAO ulistDao;
    private final EntityManager em;
    private final TaskMng taskMng;
    private final TaskParMng taskParMng;
    private final UlistMng ulistMng;
    private final EolinkDAO eolinkDao;
    private final PseudoTaskBuilder ptb;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;


    @AllArgsConstructor
    static class SoapPar {
        private NsiPortsTypeAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    /**
     * @param task - задание
     */
    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        // создать сервис и порт
        NsiServiceAsync service = new NsiServiceAsync();
        NsiPortsTypeAsync port = service.getNsiPortAsync();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);
        sb.setUp((BindingProvider) port, true, reqProp.getPpGuid(),
                reqProp.getHostIp(), true);

        // логгинг запросов
        sb.setTrace(task.getTrace().equals(1));
        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }

    /**
     * Получить состояние запроса
     *
     */
    private ru.gosuslugi.dom.schema.integration.nsi.GetStateResult getState2(Task task) throws CantPrepSoap, CantSendSoap {

        // Признак ошибки
        boolean err = false;
        // Признак ошибки в CommonResult
        boolean errChld = false;
        String errStr = null;
        ru.gosuslugi.dom.schema.integration.nsi.GetStateResult state = null;

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!
        try {
            state = par.port.getState(gs);
        } catch (ru.gosuslugi.dom.schema.integration.nsi_service_async.Fault e) {
            e.printStackTrace();
            err = true;
            errStr = "Запрос вернул ошибку!";
        }

        if (state != null && state.getRequestState() != 3) {
            // вернуться, если задание всё еще не выполнено
            log.info("Статус запроса={}, Task.id={}", state.getRequestState(), task.getId());
            if (state.getRequestState() == 1) {
                // статус запроса - ACK - увеличить время ожидания
                taskMng.alterDtNextStart(task);
            }
            return null;
        }

        // Показать ошибки, если есть
        if (err) {
            // Ошибки во время выполнения
            log.info(errStr);
            task.setState("ERR");
            task.setResult(errStr);
        } else if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null) {
            // Ошибки контролей или бизнес-процесса
            err = true;
            errStr = state.getErrorMessage().getDescription();
            log.info("Ошибка выполнения запроса = {}", errStr);
            task.setState("ERR");
            task.setResult(errStr);
        } else {

            for (CommonResultType e : state.getImportResult()) {
                for (Error f : e.getError()) {
                    // Найти элемент задания по Транспортному GUID
                    Task task2 = taskMng.getByTguid(task, e.getTransportGUID());
                    // Установить статусы ошибки по заданиям
                    task2.setState("ERR");
                    errStr = String.format("Error code=%s, Description=%s", f.getErrorCode(), f.getDescription());
                    task2.setResult(errStr);
                    log.error(errStr);

                    errChld = true;
                }
            }
        }

        if (!err) {
            // если в главном задании нет ошибок, но в любом дочернем задании обнаружена ошибка - статус - "Ошибка"
            // и если уже не установлен признак ошибки
            if (errChld && !task.getState().equals("ERR")
                    && !task.getState().equals("ERS")) {
                task.setState("ERS");
                task.setResult(errStr);
                log.error("Ошибки в элементе CommonResult");
            }
        }

        return state;

    }


    /**
     * Получить внутренний справочник организации
     */

    public Boolean exportDataProviderNsiItem(Integer taskId) throws WrongGetMethod, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);
        AckRequest ack = null;
        // для обработки ошибок
        Boolean err = false;
        String errMainStr = null;

        ExportDataProviderNsiItemRequest req = new ExportDataProviderNsiItemRequest();
        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        Double regNum = taskParMng.getDbl(task, "ГИС ЖКХ.Номер справочника");
        req.setRegistryNumber(BigInteger.valueOf(regNum.longValue()));
        try {
            ack = par.port.exportDataProviderNsiItem(req);
        } catch (ru.gosuslugi.dom.schema.integration.nsi_service_async.Fault e) {
            e.printStackTrace();
            err = true;
            errMainStr = e.getFaultInfo().getErrorMessage();
        }

        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
        }
        return err;
    }

    /**
     * Получить результат запроса внутреннего справочника организации
     */

    public void exportDataProviderNsiItemAsk(Integer taskId) throws WrongGetMethod, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        setUp(task);
        // получить состояние запроса
        ru.gosuslugi.dom.schema.integration.nsi.GetStateResult retState = getState2(task);
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
            Eolink eolink = task.getEolink();
            String grp = "NSI";
            Double regNum = taskParMng.getDbl(task, "ГИС ЖКХ.Номер справочника");
            int tp = regNum.intValue();
            // получить из нашей базы
            UlistTp ulistTp = ulistDao.getListTp(grp, eolink, tp);
            String prefix = ulistMng.getPrefixedCD(String.valueOf(tp), grp);
            if (ulistTp == null) {
                // не найден заголовок, создать новый
                ulistTp = new UlistTp();
                ulistTp.setCd(prefix);
                ulistTp.setFkExt(tp);
                ulistTp.setName("Внутренний справочник организации");
                ulistTp.setDt1(new Date());
                ulistTp.setGrp(grp);
                ulistTp.setEolink(eolink);

                em.persist(ulistTp);
                log.info("Создан заголовочный элемент U_LIST_TP Id={}, prefix={}", ulistTp.getId(), prefix);
            }
            String org = ulistTp.getEolink().getOrg().getReu();

            // загрузить полученные элементы
            Integer idx = 0;
            for (NsiElementType t : retState.getNsiItem().getNsiElement()) {
                idx = ulistMng.mergeElement(ulistTp, grp, tp, t, idx, org);
            }

            // Установить статус выполнения задания
            task.setState("ACP");
        }


    }

    /**
     * Проверить наличие заданий по экспорту справочников организации
     * и если их нет, - создать
     */

    public void checkPeriodicTask(Integer taskId) throws WrongParam {
        Task task = em.find(Task.class, taskId);
        // создать по всем организациям задания, если их нет
        // добавить как зависимое задание к системному повторяемому заданию
        String actTp = "GIS_EXP_DATA_PROVIDER_NSI_ITEM";
        String parentCD = "SYSTEM_RPT_REF_EXP";
        // создавать по 10 штук, иначе -блокировка Task (нужен коммит)
        int a = 1;
        for (Eolink e : eolinkDao.getEolinkByTpWoTaskTp("Организация", actTp, parentCD)) {
            // статус - INS, чтобы сразу выполнилось
            Task newTask = ptb.setUp(e, null, actTp, "INS", config.getCurUserGis().get().getId());
            // Справочник № 1
            ptb.addTaskPar(newTask,"ГИС ЖКХ.Номер справочника", 1D, null, null, null);
            ptb.addAsChild(newTask,parentCD);
            ptb.save(newTask);

            // Справочник № 51
            Task newTask2 = ptb.setUp(e, null, actTp, "INS", config.getCurUserGis().get().getId());
            ptb.addTaskPar(newTask2, "ГИС ЖКХ.Номер справочника", 51D, null, null, null);
            ptb.addAsChild(newTask2, parentCD);
            ptb.save(newTask2);

            log.info("Добавлено задание по экспорту справочников организации по Организации Eolink.id={}", e.getId());
            a++;
            if (a >= 100) {
                break;
            }
        }
        // Установить статус выполнения задания
        task.setState("ACP");

    }

}
