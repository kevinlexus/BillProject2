package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.enums.DebtSubRequestInnerStatuses;
import com.dic.app.enums.DebtSubRequestResponseStatuses;
import com.dic.app.enums.DebtSubRequestStatuses;
import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.service.ConfigApp;
import com.dic.bill.dao.*;
import com.dic.bill.model.exs.DebSubRequest;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.Lst;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.Tuser;
import com.dic.bill.model.scott.UserPerm;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Value;
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

import javax.annotation.PostConstruct;
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
    private final EolinkDAO2 eolinkDAO2;
    private final DebSubRequestDAO debSubRequestDAO;
    private final TuserDAO tuserDAO;
    private final UserPermDAO userPermDAO;
    private final UlstDAO ulstDAO;
    @Value("${parameters.gis.sub.request.mark.sent.on.receive}")
    private Boolean markSentOnReceive;
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

    @Getter
    @Setter
    private static class State {
        private ru.gosuslugi.dom.schema.integration.drs.GetStateResult stateResult = null;
        private boolean isErrExist = false;
        private String errorMessage = "";
        private boolean processed = false;
    }

    /**
     * Получить состояние запроса
     *
     * @param task - задание
     */
    private State getState(Task task) throws CantPrepSoap, CantSendSoap {
        // Признак ошибки
        State state = new State();
        // Признак ошибки в CommonResult

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!

        try {
            state.setStateResult(par.port.getState(gs));
        } catch (ru.gosuslugi.dom.schema.integration.drs_service_async.Fault e) {
            state.setErrorMessage(e.getFaultInfo().getErrorCode());
            state.setErrExist(true);
            e.printStackTrace();
        }

        if (state.getStateResult() != null && state.getStateResult().getRequestState() != 3) {
            // вернуться, если задание всё еще не выполнено
            log.info("Статус запроса={}, Task.id={}", state.getStateResult().getRequestState(), task.getId());

            if (state.getStateResult().getRequestState() == 1) {
                // статус запроса - ACK - увеличить время ожидания
                taskMng.alterDtNextStart(task);
            }
            return state;
        }

        state.setProcessed(true);

        // Показать ошибки, если есть
        if (state.isErrExist) {
            // Ошибки во время выполнения
            task.setState("ERR");
            log.error("Task.id={}, ОШИБКА выполнения запроса = {}", task.getId(), state.getErrorMessage());
        } else {
            if (state.getStateResult() == null) {
                log.error("ОШИБКА! stateResult==null");
            } else if (state.getStateResult().getErrorMessage() != null && state.getStateResult().getErrorMessage().getErrorCode() != null) {
                // Ошибки контролей или бизнес-процесса
                state.setErrExist(true);
                state.setErrorMessage(state.getStateResult().getErrorMessage().getDescription());
                log.info("Ошибка выполнения запроса = {}", state.getErrorMessage());
                task.setState("ERR");
                task.setResult(state.getErrorMessage());
            }
        }
        return state;
    }


    public void exportDebtSubrequest(Integer taskId) throws CantPrepSoap, DatatypeConfigurationException, CantSendSoap {
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

        if (task.getNextBlockGuid() != null) {
            req.setExportSubrequestGUID(task.getNextBlockGuid());
        } else {
            // дата начала - текущая дата - 1 месяц, дата окончания - текущая дата
            XMLGregorianCalendar startDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.now().minusMonths(1)
                    .atStartOfDay(ZoneId.systemDefault()).toInstant()));
            XMLGregorianCalendar endDate = Utl.getXMLGregorianCalendarFromDate(Date.from(LocalDate.now()
                    .atStartOfDay(ZoneId.systemDefault()).toInstant()));

            Period period = new Period();
            period.setStartDate(startDate);
            period.setEndDate(endDate);
            req.setPeriodOfSendingRequest(period);
            req.getHouseGUID().add(ObjectUtils.firstNonNull(task.getEolink().getGuidGis(), task.getEolink().getGuid()));
        }
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
        State state = getState(task);
        if (state.processed) {
            if (!task.getState().equals("ERR")) {
                // Ошибок нет, обработка
                ExportDSRsResultType exportDSRsResult = state.getStateResult().getExportDSRsResult();
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
                        if (debSubRequest.getProcUk() == null) {
                            debSubRequest.setProcUk(task.getProcUk());
                        }

                        if (isNew) {
                            // по умолчанию поля, на новой записи
                            debSubRequest.setHasDebt(false);
                            debSubRequest.setIsRevoked(false);
                            Org procUk = task.getProcUk().getOrg();
                            debSubRequest.setUk(procUk);

                            // сразу маркировать на отправку, чтоб ушло следующим запросом
                            if (markSentOnReceive || procUk.getIsAutoSendDebReq()) {
                                debSubRequest.setStatus(DebtSubRequestInnerStatuses.SENT.getId());
                            } else {
                                debSubRequest.setStatus(DebtSubRequestInnerStatuses.RECEIVED.getId());
                            }

                            debSubRequest.setRequestGuid(subrequest.getSubrequestGUID());
                            debSubRequest.setRequestNumber(requestInfo.getRequestNumber());

                            Eolink house = eolinkDAO2.findEolinkByGuid(housingFundObject.getFiasHouseGUID());
                            if (house == null)
                                house = eolinkDAO2.findEolinkByGuidGis(housingFundObject.getFiasHouseGUID());
                            debSubRequest.setHouse(house);
                            if (house == null) {
                                debSubRequest.setResult("Дом не найден в базе EOLINK, по GUID=" + housingFundObject.getFiasHouseGUID());
                            }
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
                        } else {
                            if (debSubRequest.getHouse() == null) {
                                // не был проставлен дом, по причине отсутствия по GUID
                                Eolink house = eolinkDAO2.findEolinkByGuid(housingFundObject.getFiasHouseGUID());
                                if (house == null)
                                    house = eolinkDAO2.findEolinkByGuidGis(housingFundObject.getFiasHouseGUID());
                                debSubRequest.setHouse(house);
                                if (house == null) {
                                    debSubRequest.setResult("Дом не найден в базе EOLINK, по GUID=" + housingFundObject.getFiasHouseGUID());
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

                if (pagedOutput.isLastPage() != null && !pagedOutput.isLastPage()) {
                    log.info("Ожидается следующий блок данных GUID={}", pagedOutput.getNextSubrequestGUID());
                    task.setNextBlockGuid(pagedOutput.getNextSubrequestGUID());
                    task.setState("INS"); // перевести опять в INS, чтобы принимался следующий блок данных
                } else {
                    log.info("Все блоки данных получены");
                    task.setNextBlockGuid(null);
                }

                // Установить статус выполнения задания
                task.setState("ACP");
                taskMng.logTask(task, false, true);
            }
        }
    }

    /**
     * Импорт ответов на запросы о задолженности в ГИС
     */
    public void importDebtSubrequestResponse(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        Lst lstTp = ulstDAO.getByCd("EXP_DEB_SUB_REQUEST");
        // Установить параметры SOAP
        SoapPar par = setUp(task);
        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ImportDSRResponsesRequest req = new ImportDSRResponsesRequest();

        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        List<DebSubRequest> reqs = debSubRequestDAO.getAllByStatusInAndHouseIdAndProcUkId(
                List.of(DebtSubRequestInnerStatuses.SENT.getId()), task.getEolink().getId(), task.getProcUk().getId());


        boolean existsForSending = false;
        for (DebSubRequest debSubRequest : reqs) {
            ImportDSRResponsesRequest.Action action = new ImportDSRResponsesRequest.Action();
            String tguid = Utl.getRndUuid().toString();
            action.setTransportGUID(tguid);
            debSubRequest.setTguid(tguid);
            debSubRequest.setIsErrorOnResponse(false);
            debSubRequest.setResult(null);
            debSubRequest.setStatus(DebtSubRequestInnerStatuses.PROCESSING.getId());
            action.setSubrequestGUID(debSubRequest.getRequestGuid());

            DSRResponseActionType actionType;
            if (debSubRequest.getIsRevoked()) {
                existsForSending = true;
                actionType = DSRResponseActionType.REVOKE;
            } else {
                actionType = DSRResponseActionType.SEND;
                ImportDSRResponseType responseData = new ImportDSRResponseType();
                responseData.setHasDebt(debSubRequest.getHasDebt());
                responseData.setDescription(debSubRequest.getDescription());
                List<UserPerm> userPerm = userPermDAO.findByUkReuAndTpCd(debSubRequest.getUk().getReu(), lstTp.getCd());
                if (userPerm.size() == 0) {
                    debSubRequest.setIsErrorOnResponse(true);
                    debSubRequest.setResult("Не определён пользователь, подписывающий документ, в справочнике пользователей");
                    log.error("По DEBT_SUB_REQUEST.ID={}, не определён пользователь, подписывающий документ, в справочнике пользователей", debSubRequest.getId());
                    continue;
                } else if (userPerm.size() > 1) {
                    debSubRequest.setIsErrorOnResponse(true);
                    debSubRequest.setResult("Кол-во подписывающих документ > 1 в справочнике пользователей");
                    log.error("По DEBT_SUB_REQUEST.ID={}, кол-во подписывающих документ > 1 в справочнике пользователей", debSubRequest.getId());
                    continue;
                } else {
                    Tuser userSigner = userPerm.get(0).getUser();
                    if (StringUtils.isEmpty(userSigner.getGuid())) {
                        debSubRequest.setIsErrorOnResponse(true);
                        debSubRequest.setResult("У подписывающего документ пользователя, не заполнен GUID, в справочнике пользователей");
                        log.error("По DEBT_SUB_REQUEST.ID={}, у подписывающего документ пользователя, не заполнен GUID, в справочнике пользователей", debSubRequest.getId());
                        continue;
                    } else {
                        existsForSending = true;
                        responseData.setExecutorGUID(userSigner.getGuid());
                    }
                }

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

        if (existsForSending) {
            try {
                ack = par.port.importResponses(req);
            } catch (Exception e1) {
                e1.printStackTrace();
                err = true;
                errMainStr = e1.getMessage();
            }
            if (err) {
                State state = new State();
                state.setErrExist(true);
                state.setErrorMessage("Ошибка при отправке XML: " + errMainStr);
                markRequestsError(task, state);
                task.setState("ERR");
                task.setResult(state.getErrorMessage());
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
        State state = getState(task);
        if (state.processed) {
            if (state.isErrExist) {
                // есть ошибки отправки запроса (например 400, Bad request)
                markRequestsError(task, state);
            } else {
                // Ошибок отправки нет, обработка
                List<CommonResultType> importResult = state.getStateResult().getImportResult();

                for (CommonResultType commonResultType : importResult) {
                    String tguid = commonResultType.getTransportGUID();
                    Optional<DebSubRequest> debSubRequestOpt = debSubRequestDAO.getByTguid(tguid);
                    debSubRequestOpt.ifPresent(t -> t.setStatus(DebtSubRequestInnerStatuses.RECEIVED.getId()));
                    debSubRequestOpt.ifPresent(t -> t.setResult(null));
                    debSubRequestOpt.ifPresent(t -> t.setIsErrorOnResponse(false));

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

    private void markRequestsError(Task task, State state) {
        List<DebSubRequest> debSubRequests = debSubRequestDAO.getAllByStatusInAndHouseIdAndProcUkId(
                List.of(DebtSubRequestInnerStatuses.PROCESSING.getId()), task.getEolink().getId(), task.getProcUk().getId());
        debSubRequests.forEach(t -> {
            t.setIsErrorOnResponse(true);
            t.setResult(state.errorMessage);
            t.setStatus(DebtSubRequestInnerStatuses.RECEIVED.getId());
        });
    }


}
