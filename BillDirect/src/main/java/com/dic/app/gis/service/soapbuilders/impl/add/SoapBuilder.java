package com.dic.app.gis.service.soapbuilders.impl.add;

import com.dic.app.gis.service.soap.impl.LoggingSOAPHandler;
import com.dic.app.gis.service.soapbuilders.impl.add.SoapConfig;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.CantSendSoap;
import com.sun.xml.ws.developer.WSBindingProvider;
import lombok.extern.slf4j.Slf4j;
import org.apache.xml.security.utils.Base64;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import ru.gosuslugi.dom.schema.integration.base.ISRequestHeader;
import ru.gosuslugi.dom.schema.integration.base.RequestHeader;

import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.Binding;
import javax.xml.ws.BindingProvider;
import javax.xml.ws.handler.Handler;
import javax.xml.ws.handler.MessageContext;
import java.net.MalformedURLException;
import java.net.UnknownHostException;
import java.util.*;

/**
 * Вспомогательный класс для постройки SOAP запроса
 *
 * @author lev
 */
@Service
@Scope("prototype")
@Slf4j
public class SoapBuilder  {
    @Autowired
    private SoapConfig config;

    private BindingProvider bp;
    private WSBindingProvider ws;


    public void makeRndMsgGuid() {
    }

    // подписывать ли XML

    public void setSign(boolean sign) {
        // подписывать ли XML?
        if (sign) {
            bp.getRequestContext().put("sign", "");
        } else {
            bp.getRequestContext().remove("sign", "");
        }

    }

    // выбрать объект подписывания XML

    public void setSignerId(int signerId) {
        bp.getRequestContext().put("signerId", String.valueOf(signerId));
    }

    // логгировать ли обмен

    public void setTrace(boolean trace) {
        if (trace) {
            bp.getRequestContext().put("trace", "");
        } else {
            bp.getRequestContext().remove("trace", "");
        }
    }

    /**
     * Инициализация
     *
     * @param sign   - подписать XML?
     * @param hostIp - Ip адрес хоста
     */

    public void setUp(BindingProvider port, WSBindingProvider port2, boolean sign,
                      String ppGuid, String hostIp) throws CantSendSoap {
        RequestHeader rh = new RequestHeader();
        UUID messGUID = Utl.getRndUuid();
        rh.setMessageGUID(messGUID.toString());

        setUp(port, null, rh, sign, true, ppGuid, hostIp);
    }

    /**
     * Инициализация
     *
     * @param sign   - подписать XML?
     * @param hostIp - Ip адрес хоста
     */

    public void setUpSimple(BindingProvider port, WSBindingProvider port2, boolean sign,
                            String ppGuid, String hostIp) throws CantSendSoap {
        ISRequestHeader rhSimple = new ISRequestHeader();
        UUID messGUID = Utl.getRndUuid();
        rhSimple.setMessageGUID(messGUID.toString());
        setUp(port, rhSimple, null, sign, false, ppGuid, hostIp);
    }

    /**
     * Инициализация
     *
     * @param sign   - подписать XML?
     * @param hostIp - Ip адрес хоста
     */
    private void setUp(BindingProvider port, ISRequestHeader rhSimple,
                       RequestHeader rh, boolean sign,
                       boolean isOperatorSigned, String ppGuid, String hostIp) throws CantSendSoap {
        bp = port;
        ws = (WSBindingProvider) port;

        // подпись
        setSign(sign);

        // установить Random Message GUID и дату
        GregorianCalendar c = new GregorianCalendar();
        c.setTime(new Date());
        XMLGregorianCalendar cl;
        try {
            cl = DatatypeFactory.newInstance().newXMLGregorianCalendar(c);
        } catch (DatatypeConfigurationException e1) {
            e1.printStackTrace();
            throw new CantSendSoap("Ошибка при подготовке даты для SOAP Message");
        }

        UUID messGUID = Utl.getRndUuid();
        if (rhSimple != null) {
            rhSimple.setDate(cl);
            rhSimple.setMessageGUID(messGUID.toString());
            ws.setOutboundHeaders(rhSimple);
        } else {
            rh.setDate(cl);
            rh.setMessageGUID(messGUID.toString());
            if (isOperatorSigned) {
                // подпись оператора?
                rh.setIsOperatorSignature(isOperatorSigned);
            }
            if (ppGuid != null) {
                // GUID УК от которой выполняется запрос
                rh.setOrgPPAGUID(ppGuid);
            }
            ws.setOutboundHeaders(rh);
        }

        String urlStr = (String) bp.getRequestContext().get(BindingProvider.ENDPOINT_ADDRESS_PROPERTY);
        String path;
        try {
            path = Utl.getPathFromUrl(urlStr);
        } catch (UnknownHostException | MalformedURLException e) {
            e.printStackTrace();
            throw new CantSendSoap("Ошибка при определении хоста");
        }

        Map<String, List<String>> requestHeaders = new HashMap<>();
        @SuppressWarnings("restriction")
        //String authorization = new sun.misc.BASE64Encoder().encode((config.getBscLogin() + ":" + config.getBscPass()).getBytes());
        String authorization = Base64.encode((config.getBscLogin() + ":" + config.getBscPass()).getBytes());
        requestHeaders.put("Authorization", Collections.singletonList("Basic " + authorization));
        requestHeaders.put("X-Client-Cert-Fingerprint", Collections.singletonList(config.getFingerPrint()));

        log.trace("************* SoapBuilder: endpoint: hostIp={}{}", hostIp, path);
        bp.getRequestContext().put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY,
                "http://" + hostIp + path);
        bp.getRequestContext().put(MessageContext.HTTP_REQUEST_HEADERS, requestHeaders);

        // добавить хэндлер, для установщика подписи ЭЦП
        Binding binding = bp.getBinding();
        List<Handler> handlerChain = binding.getHandlerChain();
        handlerChain.add(new LoggingSOAPHandler());
        binding.setHandlerChain(handlerChain);
    }

}
