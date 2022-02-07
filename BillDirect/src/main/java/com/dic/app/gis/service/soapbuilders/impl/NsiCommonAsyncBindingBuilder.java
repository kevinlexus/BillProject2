package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.model.exs.Task;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.CantSignSoap;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiItemType;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiListType;
import ru.gosuslugi.dom.schema.integration.nsi_common.ExportNsiItemRequest;
import ru.gosuslugi.dom.schema.integration.nsi_common.ExportNsiListRequest;
import ru.gosuslugi.dom.schema.integration.nsi_common.GetStateResult;
import ru.gosuslugi.dom.schema.integration.nsi_common_service_async.NsiPortsTypeAsync;
import ru.gosuslugi.dom.schema.integration.nsi_common_service_async.NsiServiceAsync;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.ws.BindingProvider;
import java.math.BigInteger;

@Service
@Slf4j
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class NsiCommonAsyncBindingBuilder {

    @PersistenceContext
    private EntityManager em;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;


    @AllArgsConstructor
    static class SoapPar {
        private NsiPortsTypeAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        // создать сервис и порт
        NsiServiceAsync service = new NsiServiceAsync();
        NsiPortsTypeAsync port = service.getNsiPortAsync();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);

        sb.setUpWithISRequestHeader((BindingProvider) port, false, reqProp.getHostIp());

        // логгинг запросов
        sb.setTrace(task.getTrace().equals(1));
        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }


    /**
     * Получить список справочников
     *
     * @param grp - вид справочника (NSI, NISRAO)
     */
    public NsiListType getNsiList(Integer taskId, String grp) throws CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault, CantPrepSoap {
        // Установить параметры SOAP
        Task task = em.find(Task.class, taskId);
        SoapPar par = setUp(task);
        par.sb.setSign(true);

        ExportNsiListRequest req = new ExportNsiListRequest();

        req.setId("foo");
        req.setListGroup(grp);

        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        GetStateRequest gs = new GetStateRequest();

        AckRequest ack = par.port.exportNsiList(req);

        gs.setMessageGUID(ack.getAck().getMessageGUID());
        GetStateResult state = null;

        par = setUp(task);
        par.sb.setSign(false);

        while (state == null || state.getRequestState() != 3) {

            state = par.port.getState(gs);
            log.info("Состояние запроса state = {}", state.getRequestState());
            // задержка 1 сек
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null) {
            log.info("Вложенная ошибка XML: {}", state.getErrorMessage().getDescription());
            throw new CantSendSoap(state.getErrorMessage().getDescription());
        }
        return state.getNsiList();
    }

    /**
     * Получить справочник
     *
     * @param grp - вид справочника (NSI, NISRAO)
     */

    public NsiItemType getNsiItem(Integer taskId, String grp, BigInteger id) throws CantSignSoap, CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault, CantPrepSoap {
        Task task = em.find(Task.class, taskId);
        SoapPar par = setUp(task);

        ExportNsiItemRequest req = new ExportNsiItemRequest();
        req.setListGroup(grp);
        req.setRegistryNumber(id);
        req.setId("foo");
        par.sb.setSign(true);
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        AckRequest ack = par.port.exportNsiItem(req);
        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(ack.getAck().getMessageGUID());
        GetStateResult state = null;

        par.sb.setSign(false);
        par.sb.setTrace(true);

        par = setUp(task);
        while (state == null || state.getRequestState() != 3) {

            state = par.port.getState(gs);
            log.info("Состояние запроса state = {}", state.getRequestState());
            // задержка 1 сек
            try {
                Thread.sleep(200);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null) {
            log.info("Вложенная ошибка XML: код={},  описание={}", state.getErrorMessage().getErrorCode(), state.getErrorMessage().getDescription());
            if (state.getErrorMessage().getErrorCode().equals("INT016001") ||
                    state.getErrorMessage().getErrorCode().equals("INT016041")
            ) {
                // Справочник пустой в ГИС, не загружен!
                return null;
            } else {
                throw new CantSendSoap(state.getErrorMessage().getDescription());
            }
        }
        return state.getNsiItem();
    }


}
