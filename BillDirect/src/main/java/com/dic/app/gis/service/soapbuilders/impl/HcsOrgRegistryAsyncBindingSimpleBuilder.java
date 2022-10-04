package com.dic.app.gis.service.soapbuilders.impl;

import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.service.ConfigApp;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;
import com.sun.xml.ws.developer.WSBindingProvider;
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
import ru.gosuslugi.dom.schema.integration.organizations_registry_common.ExportOrgRegistryRequest;
import ru.gosuslugi.dom.schema.integration.organizations_registry_common.ExportOrgRegistryRequest.SearchCriteria;
import ru.gosuslugi.dom.schema.integration.organizations_registry_common.GetStateResult;
import ru.gosuslugi.dom.schema.integration.organizations_registry_common_service_async.RegOrgPortsTypeAsync;
import ru.gosuslugi.dom.schema.integration.organizations_registry_common_service_async.RegOrgServiceAsync;

import javax.persistence.EntityManager;
import javax.xml.ws.BindingProvider;

@Service
@Slf4j
@RequiredArgsConstructor
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
public class HcsOrgRegistryAsyncBindingSimpleBuilder {

    private final TaskMng taskMng;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;
    private final EntityManager em;

    @AllArgsConstructor
    static class SoapPar {
        private RegOrgPortsTypeAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        // создать сервис и порт
        RegOrgServiceAsync service = new RegOrgServiceAsync();
        RegOrgPortsTypeAsync port = service.getRegOrgAsyncPort();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);
        sb.setUpSimple((BindingProvider) port, (WSBindingProvider) port, true, reqProp.getPpGuid(),
                reqProp.getHostIp());

        // логгинг запросов
        sb.setTrace(task.getTrace().equals(1));
        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }

    /**
     * Получить состояние запроса
     *
     * @param task - задание
     */
    private GetStateResult getState2(Task task) throws CantPrepSoap, CantSendSoap {
        // Признак ошибки
        boolean err = false;
        // Признак ошибки в CommonResult
        boolean errChld = false;
        String errStr = null;
        GetStateResult state = null;

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!
        try {
            state = par.port.getState(gs);
        } catch (ru.gosuslugi.dom.schema.integration.organizations_registry_common_service_async.Fault e) {
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
     * Экспорт данных организации
     */
    public boolean exportOrgRegistry(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        //reqProp.setPropWOGUID(task, sb);
        //sb.setTrace(task != null ? task.getTrace().equals(1) : false);
        // Установить параметры SOAP без GUID (сделал
        SoapPar par = setUp(task);

        AckRequest ack = null;
        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;
        Eolink eolOrg = task.getEolink();

        ExportOrgRegistryRequest req = new ExportOrgRegistryRequest();

        req.setId("foo");
        par.sb.setSign(true);
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        if (eolOrg.getOgrn() != null) {
            SearchCriteria sc = new SearchCriteria();
            sc.setOGRN(eolOrg.getOgrn());
            sc.setIsRegistered(true);
            req.getSearchCriteria().add(sc);

            try {
                ack = par.port.exportOrgRegistry(req);
            } catch (ru.gosuslugi.dom.schema.integration.organizations_registry_common_service_async.Fault e1) {
                e1.printStackTrace();
                err = true;
                errMainStr = "Ошибка выполнения основного SOAP запроса!";
            }
        } else {
            // Не заполнен ОГРН
            err = true;
            errMainStr = "Отсутствует ОГРН!";
        }

        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
            taskMng.logTask(task, false, false);

        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
            taskMng.logTask(task, false, true);

        }

        return err;
    }

    /**
     * Получить результат экспорта параметров организации
     */
    public void exportOrgRegistryAsk(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // установить параметры SOAP
        //reqProp.setPropWOGUID(task, sb);

        SoapPar par = setUp(task);
        Eolink eolOrg = task.getEolink();

        // получить состояние запроса
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {

            retState.getExportOrgRegistryResult().forEach(t -> {
                if (eolOrg.getGuid() == null) {
                    eolOrg.setGuid(t.getOrgPPAGUID());
                    log.info("По Организации: Eolink.id={} сохранен GUID={}", eolOrg.getId(), t.getOrgPPAGUID());
                } else {
                    log.info("По Организации: Eolink.id={} получен GUID={}", eolOrg.getId(), t.getOrgPPAGUID());
                }
            });

            // Установить статус выполнения задания
            task.setState("ACP");
            taskMng.logTask(task, false, true);
        }
    }
}
