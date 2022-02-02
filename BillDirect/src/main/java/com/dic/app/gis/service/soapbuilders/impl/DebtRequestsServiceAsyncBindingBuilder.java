package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.*;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.SoapConfigs;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.UlistDAO;
import com.dic.bill.dao.*;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.LstMng;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.UnusableCode;
import com.ric.cmn.excp.WrongGetMethod;
import com.sun.xml.ws.developer.WSBindingProvider;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.base.Period;
import ru.gosuslugi.dom.schema.integration.drs.*;
import ru.gosuslugi.dom.schema.integration.drs_service_async.DebtRequestsAsyncPort;
import ru.gosuslugi.dom.schema.integration.drs_service_async.DebtRequestsServiceAsync;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.Date;

/**
 * Сервис обмена информацией с ГИС ЖКХ по Дому
 */
@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class DebtRequestsServiceAsyncBindingBuilder {

    private final ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;
    private final UlistMng ulistMng;
    private final ConfigApp config;
    private final TaskParMng taskParMng;
    private final EolinkDAO eolinkDao;
    private final EolinkDAO2 eolinkDao2;
    private final AddrTpDAO addrTpDAO;
    private final OrgDAO orgDAO;
    private final UlistDAO ulistDAO;
    private final EolinkParMng eolinkParMng;
    private final TaskEolinkParMng teParMng;
    private final TaskDAO taskDao;
    private final KartMng kartMng;
    private final EolinkMng eolinkMng;
    private final TaskMng taskMng;
    private final LstMng lstMng;
    private final SoapConfigs soapConfig;
    private final MeterMng meterMng;
    private final PseudoTaskBuilder ptb;
    private final EolinkParMng eolParMng;

    @Getter
    @Setter
    static class LskEolParam {
        String accountGUID;
        String accountNumber;
        String unifiedAccountNumber;
        String serviceID;
        Eolink eolink;
        Boolean isClosed;
    }

    @AllArgsConstructor
    static class SoapPar {
        private DebtRequestsAsyncPort port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    /**
     * Инициализация - создать сервис и порт
     */
    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        DebtRequestsServiceAsync service = new DebtRequestsServiceAsync();
        DebtRequestsAsyncPort port = service.getDebtRequestsAsyncPort();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);
        sb.setUp((BindingProvider) port, (WSBindingProvider) port, true, reqProp.getPpGuid(),
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
     * @return
     */
    private GetStateResult getState2(Task task) throws CantPrepSoap, CantSendSoap {
        // Признак ошибки
        boolean err = false;
        // Признак ошибки в CommonResult
        String errStr = null;
        ru.gosuslugi.dom.schema.integration.drs.GetStateResult state = null;

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!

        String errMsg = null;
        try {
            state = par.port.getState(gs);
        } catch (ru.gosuslugi.dom.schema.integration.drs_service_async.Fault e) {
            errMsg = e.getFaultInfo().getErrorCode();
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
            task.setResult(errMsg);
            log.error("Task.id={}, ОШИБКА выполнения запроса = {}", task.getId(), errStr);
        } else {
            if (state == null) {
                log.error("ОШИБКА! state==null");
            } else if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null) {
                // Ошибки контролей или бизнес-процесса
                errStr = state.getErrorMessage().getDescription();
                log.info("Ошибка выполнения запроса = {}", errStr);
                task.setState("ERR");
                task.setResult(errStr);
            }
        }

        return state;

    }


    public void exportDebtSubrequests(Integer taskId) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);

        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ExportDSRsRequest req = new ExportDSRsRequest();

        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        Period period = new Period();
        XMLGregorianCalendar startDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.of(2021, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC)));
        period.setStartDate(startDate);
        XMLGregorianCalendar endDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.of(2022, 2, 1).atStartOfDay().toInstant(ZoneOffset.UTC)));
        period.setEndDate(endDate);
        req.setPeriodOfSendingRequest(period);
        //req.getRequestStatus().add(RequestStatusType.SENT);

        try {
            ack = par.port.exportDebtSubrequests(req);
        } catch (ru.gosuslugi.dom.schema.integration.drs_service_async.Fault e1) {
            e1.printStackTrace();
            err = true;
            errMainStr = e1.getFaultInfo().getErrorMessage();
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

    }


    public void exportDebtRequests(Integer taskId) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);

        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ExportDRsRequest req = new ExportDRsRequest();

        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        Period period = new Period();
        XMLGregorianCalendar startDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.of(2021, 12, 1).atStartOfDay().toInstant(ZoneOffset.UTC)));
        period.setStartDate(startDate);
        XMLGregorianCalendar endDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.of(2022, 2, 1).atStartOfDay().toInstant(ZoneOffset.UTC)));
        period.setEndDate(endDate);

        req.setRequestCreationPeriod(period);
        req.setPeriodOfSendingRequest(period);
        //req.getRequestStatus().add(AllRequestStatusesType.SENT);

        try {
            ack = par.port.exportDebtRequests(req);
        } catch (ru.gosuslugi.dom.schema.integration.drs_service_async.Fault e1) {
            e1.printStackTrace();
            err = true;
            errMainStr = e1.getFaultInfo().getErrorMessage();
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

    }


    public void exportDebtSubrequestsAsk(Integer taskId) throws UnusableCode, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);

        // дом
        Eolink houseEol = task.getEolink();
        // получить состояние
        GetStateResult retState = getState2(task);
        Date curDate = new Date();
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {
            // Ошибок нет, обработка
            ExportDSRsResultType result = retState.getExportDSRsResult();
            log.info("result={}", result);
            // Установить статус выполнения задания
            task.setState("ACP");
            //log.info("******* Task.id={}, экспорт объектов дома выполнен", task.getId());
            taskMng.logTask(task, false, true);
        }
    }

}
