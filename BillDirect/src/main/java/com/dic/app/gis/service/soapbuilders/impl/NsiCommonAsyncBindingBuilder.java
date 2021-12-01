package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.EolinkMng;
import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.mm.ConfigApp;
import com.ric.cmn.excp.CantPrepSoap;
import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.CantSignSoap;
import com.sun.xml.ws.developer.WSBindingProvider;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
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

import javax.xml.ws.BindingProvider;
import java.math.BigInteger;

@Service
@Slf4j
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class NsiCommonAsyncBindingBuilder {

    private final ApplicationContext ctx;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;
    private final EolinkMng eolinkMng;


    @AllArgsConstructor
    static class SoapPar {
        private NsiPortsTypeAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    private SoapPar setUp() throws CantSendSoap, CantPrepSoap {
        // создать сервис и порт
        NsiServiceAsync service = new NsiServiceAsync();
        NsiPortsTypeAsync port = service.getNsiPortAsync();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, eolinkMng, eolParMng);
        sb.setUp((BindingProvider) port, (WSBindingProvider) port, true, reqProp.getPpGuid(),
                reqProp.getHostIp());

        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }


    /**
     * Получить список справочников
     *
     * @param grp - вид справочника (NSI, NISRAO)
     */
    public NsiListType getNsiList(String grp) throws CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault, CantPrepSoap {
        // Установить параметры SOAP
        SoapPar par = setUp();

        ExportNsiListRequest req = new ExportNsiListRequest();
        req.setListGroup(grp);
        req.setId("foo");
        par.sb.setSign(true);

        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        GetStateRequest gs = new GetStateRequest();
        AckRequest ack = par.port.exportNsiList(req);
        gs.setMessageGUID(ack.getAck().getMessageGUID());
        GetStateResult state = null;

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

    public NsiItemType getNsiItem(String grp, BigInteger id) throws CantSignSoap, CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault, CantPrepSoap {
        // выполнить инициализацию
        // Установить параметры SOAP
        SoapPar par = setUp();

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
        par.sb.setTrace(false);
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
