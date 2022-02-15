package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.enums.DebtSubRequestInnerStatuses;
import com.dic.app.enums.DebtSubRequestResponseStatuses;
import com.dic.app.enums.DebtSubRequestStatuses;
import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.dao.DebSubRequestDAO;
import com.dic.bill.dao.EolinkDAO;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.model.exs.DebSubRequest;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.Tuser;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.base.Period;
import ru.gosuslugi.dom.schema.integration.drs.*;
import ru.gosuslugi.dom.schema.integration.drs_service_async.DebtRequestsAsyncPort;
import ru.gosuslugi.dom.schema.integration.drs_service_async.DebtRequestsServiceAsync;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiRef;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Сервис обмена информацией с ГИС ЖКХ по Дому
 */
@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class DebtRequestsServiceAsyncBindingBuilder {

    @PersistenceContext
    private EntityManager em;
    private final ConfigApp config;
    private final TaskMng taskMng;
    private final EolinkParMng eolParMng;
    private final EolinkDAO eolinkDAO;
    private final DebSubRequestDAO debSubRequestDAO;
    private final TuserDAO tuserDAO;

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
     * @param task - задание
     * @return
     */
    private GetStateResult getState(Task task) throws CantPrepSoap, CantSendSoap {
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


    public void exportDebtSubrequest(Integer taskId) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException, CantSendSoap {
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


        // дата начала - текущая дата - 2 месяца, дата окончания - текущая дата
        XMLGregorianCalendar startDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.now().minusMonths(2).atStartOfDay(ZoneId.systemDefault()).toInstant()));
        XMLGregorianCalendar endDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.now().atStartOfDay(ZoneId.systemDefault()).toInstant()));

        Period period = new Period();
        period.setStartDate(startDate);
        period.setEndDate(endDate);
        req.setPeriodOfSendingRequest(period);

        req.getHouseGUID().add(task.getEolink().getGuid());
        req.setIncludeResponses(true);

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

    /*  Выгрузка запросов о задолженности из ГИС
    Работа статусов:
            status_gis = sent        status = received
            status_gis = sent        status = sent      - изменили и отправили
            status_gis = sent        status = accepted
            status_gis = sent        status = sent      - изменили и отправили
            status_gis = sent        status = accepted
            status_gis = sent        status = revoked   - отменили

    •	элементы debtInfo и additionalFile могут быть указаны, только при наличии задолженности, подтвержденной судебным актом (т.е. когда элемент hasDebt принимает значение TRUE);
    •	при наличии задолженности, подтвержденной судебным актом (т.е. когда элемент hasDebt принимает значение TRUE), необходимо указать хотя бы один элемент debtInfo;
    •	идентификатор исполнителя (executorGUID) можно посмотреть в личном кабинете ГИС ЖКХ в разделе "Администрирование" / "Сотрудники".
    */
    public void exportDebtSubrequestAsk(Integer taskId) throws UnusableCode, CantPrepSoap, CantSendSoap, WrongParam {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние
        GetStateResult retState = getState(task);
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {
            // Ошибок нет, обработка
            ExportDSRsResultType exportDSRsResult = retState.getExportDSRsResult();

            ExportDSRsResultType.PagedOutput pagedOutput = exportDSRsResult.getPagedOutput(); // todo подумать!
            for (DSRType subrequest : exportDSRsResult.getSubrequestData()) {
                DSRType.RequestInfo requestInfo = subrequest.getRequestInfo();
                if (requestInfo != null) {
                    ExportHousingFundObjectInfoType housingFundObject = requestInfo.getHousingFundObject();

                    DebSubRequest debSubRequest;
                    Optional<DebSubRequest> requestOpt = debSubRequestDAO.getByRequestGuid(subrequest.getSubrequestGUID());
                    boolean isNew = false;
                    if (requestOpt.isEmpty()) {
                        debSubRequest = new DebSubRequest();
                        isNew = true;
                    } else {
                        debSubRequest = requestOpt.get();
                    }

                    // статус запроса в ГИС
                    debSubRequest.setStatusGis(DebtSubRequestStatuses.getByName(requestInfo.getStatus().value()).getId());

                    if (isNew) {
                        // по умолчанию поля, на новой записи
                        debSubRequest.setHasDebt(false);
                        debSubRequest.setIsRevoked(false);
                        debSubRequest.setUk(task.getProcUk().getOrg());

                        debSubRequest.setStatus(DebtSubRequestInnerStatuses.RECEIVED.getId());
                        debSubRequest.setRequestGuid(subrequest.getSubrequestGUID());
                        debSubRequest.setRequestNumber(requestInfo.getRequestNumber());

                        Eolink house = eolinkDAO.getEolinkByGuid(housingFundObject.getFiasHouseGUID());
                        debSubRequest.setHouse(house);
                        debSubRequest.setAddress(housingFundObject.getAddress());
                        debSubRequest.setAddressDetail(housingFundObject.getAddressDetails());
                        debSubRequest.setSentDate(Utl.getDateFromXmlGregCal(requestInfo.getSentDate())); // отправлено
                        debSubRequest.setResponseDate(Utl.getDateFromXmlGregCal(requestInfo.getResponseDate())); // крайняя дата ответа

                        // организация, направившая запрос
                        OrganizationInfoType organization = requestInfo.getOrganization();
                        debSubRequest.setOrgFromGuid(organization.getOrgRootGUID());
                        debSubRequest.setOrgFromName(organization.getName());
                        debSubRequest.setOrgFromPhone(organization.getTel());
                        // исполнитель, направивший запрос
                        debSubRequest.setExecutorGUID(requestInfo.getExecutorInfo().getGUID());
                        debSubRequest.setExecutorFIO(requestInfo.getExecutorInfo().getFio());

                        // ответ
                        DSRType.ResponseData responseData = subrequest.getResponseData();
                        if (responseData != null) {
                            debSubRequest.setHasDebt(responseData.isHasDebt());
                            debSubRequest.setDescription(responseData.getDescription());
                            ExecutorInfoType executorInfo = responseData.getExecutorInfo();
                            if (executorInfo != null) {
                                Optional<Tuser> executorOpt = tuserDAO.getByGuid(executorInfo.getGUID());
                                if (debSubRequest.getUser() == null) {
                                    executorOpt.ifPresent(debSubRequest::setUser);
                                }
                            }
                        }
                    }
                    // статус ответа, как он отображен в ГИС
                    if (subrequest.getResponseStatus() != null) {
                        debSubRequest.setResponseStatus(DebtSubRequestResponseStatuses.getByName(subrequest.getResponseStatus().value()).getId());
                    }
                    debSubRequestDAO.save(debSubRequest);
                } else {
                    log.error("DSRType.RequestInfo requestInfo - пустой");
                }
            }
            // Установить статус выполнения задания
            task.setState("ACP");
            taskMng.logTask(task, false, true);
        }
    }

    /**
     * Импорт ответов на запросы о задолженности в ГИС
     */
    public void importDebtSubrequestResponse(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);
        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ImportDSRResponsesRequest req = new ImportDSRResponsesRequest();

        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        List<DebSubRequest> reqs = debSubRequestDAO.getAllByStatusInAndHouseId(
                List.of(DebtSubRequestInnerStatuses.SENT.getId()), task.getEolink().getId());


        boolean existsForSending = false;
        for (DebSubRequest debSubRequest : reqs) {
            if (debSubRequest.getUser() != null) {
                ImportDSRResponsesRequest.Action action = new ImportDSRResponsesRequest.Action();
                String tguid = Utl.getRndUuid().toString();
                action.setTransportGUID(tguid);
                debSubRequest.setTguid(tguid);
                debSubRequest.setIsErrorOnResponse(false);
                debSubRequest.setResult(null);
                action.setSubrequestGUID(debSubRequest.getRequestGuid());

                DSRResponseActionType actionType;
                if (debSubRequest.getIsRevoked()) {
                    actionType = DSRResponseActionType.REVOKE;
                } else {
                    actionType = DSRResponseActionType.SEND;
                    ImportDSRResponseType responseData = new ImportDSRResponseType();
                    responseData.setHasDebt(debSubRequest.getHasDebt());
                    responseData.setDescription(debSubRequest.getDescription());
                    responseData.setExecutorGUID(debSubRequest.getUser().getGuid());

                    if (debSubRequest.getFirstName() != null) {
                        // добавить задолжника
                        DebtInfoType debtInfo = new DebtInfoType();
                        DebtInfoType.Person person = new DebtInfoType.Person();
                        person.setFirstName(debSubRequest.getFirstName());
                        person.setLastName(debSubRequest.getLastName());
                        person.setMiddleName(debSubRequest.getMiddleName());
                        person.setSnils(debSubRequest.getSnils());

                        if (debSubRequest.getDocTypeGUID() != null && debSubRequest.getDocNumber() != null && debSubRequest.getDocSeria() != null) {
                            // документ должника (НСИ 95)
                            DocumentType document = new DocumentType();
                            document.setNumber(debSubRequest.getDocNumber());
                            document.setSeries(debSubRequest.getDocSeria());
                            NsiRef docType = new NsiRef();
                            docType.setGUID(debSubRequest.getDocTypeGUID());
                            // docType.setCode(); todo если свалится запрос, то попробовать заполнять эти поля
                            // docType.setName();

                            document.setType(docType);
                            person.setDocument(document);
                        }

                        /* todo можно заполнять документ, подтверждающий задолженность (НСИ 358), пока не стал делать
                                                DebtInfoType.Document doc;
                                                debtInfo.getDocument().add(doc);
                        */
                        debtInfo.setPerson(person);
                        responseData.getDebtInfo().add(debtInfo);
                    }
                    action.setResponseData(responseData);
                }
                action.setActionType(actionType);

                req.getAction().add(action);
            }
            existsForSending = true;
        }

        if (existsForSending) {
            try {
                ack = par.port.importResponses(req);
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
        } else {
            // Установить статус "Выполнено"
            log.info("Ответов для отправки не обнаружено");
            task.setState("ACP");
            task.setResult(null);
            taskMng.logTask(task, false, true);
        }


    }


    public void importDebtSubrequestResponseAsk(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние
        GetStateResult retState = getState(task);
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {
            // Ошибок нет, обработка
            List<CommonResultType> importResult = retState.getImportResult();

            for (CommonResultType commonResultType : importResult) {
                String tguid = commonResultType.getTransportGUID();
                Optional<DebSubRequest> debSubRequestOpt = debSubRequestDAO.getByTguid(tguid);
                debSubRequestOpt.ifPresent(t -> t.setStatus(DebtSubRequestInnerStatuses.RECEIVED.getId()));

                for (CommonResultType.Error error : commonResultType.getError()) {
                    debSubRequestOpt.ifPresent(t -> {
                        t.setResult(error.getDescription());
                        t.setIsErrorOnResponse(true);
                    });
                    if (debSubRequestOpt.isEmpty()) {
                        log.error("Не найден DebSubRequest по tguid={}", tguid);
                    }
                }
            }

            // Установить статус выполнения задания
            task.setState("ACP");
            taskMng.logTask(task, false, true);
        }
    }


}
