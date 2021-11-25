package com.dic.app.gis.service.soapbuilders.impl;


import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import com.dic.app.gis.service.soapbuilders.NsiCommonBindingBuilders;
import com.dic.app.gis.service.maintaners.ReqProps;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.gis.service.soap.impl.SoapConfig;
import lombok.extern.slf4j.Slf4j;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.CantSignSoap;

@Service
@Slf4j
public class NsiCommonBindingBuilder implements NsiCommonBindingBuilders {

	@Autowired
	private ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;
	@Autowired
	private SoapConfig config;
	@Autowired
	private ReqProps reqProp;

//	private NsiService service;
	//private NsiPortsType port;
	private SoapBuilder sb;

	private void setUp() throws CantSendSoap {
    	// создать сервис и порт
//		service = new NsiService();
//    	port = service.getNsiPort();

    	// подоготовительный объект для SOAP
		/*  	SoapBuilder sb = ctx.getBean(SoapBuilder.class);
		sb.setUp((BindingProvider) port, (WSBindingProvider) port, false, config.getOrgPPGuid(), config.getHostIp());
		// Id XML подписчика
		sb.setSignerId(reqProp.getSignerId());*/
	}

/*
	*/
/**
	 * Получить список справочников
	 * @param grp - вид справочника (NSI, NISRAO)
	 * @throws Fault
	 * @throws CantSendSoap
	 * @throws CantSignSoap
	 * @throws Exception
	 *//*

	public ExportNsiListResult getNsiList(String grp) throws Fault, CantSignSoap, CantSendSoap {
		// выполнить инициализацию
		setUp();

		ExportNsiListRequest req = new ExportNsiListRequest();
		req.setListGroup(grp);
		req.setId("foo");
		req.setVersion(req.getVersion()==null?reqProp.getGisVersion():req.getVersion());


		ExportNsiListResult ex = port.exportNsiList(req);
		return ex;
	}

	*/
/**
	 * Получить справочник
	 * @param grp - вид справочника (NSI, NISRAO)
	 * @throws Fault
	 * @throws CantSendSoap
	 * @throws CantSignSoap
	 * @throws Exception
	 *//*

	public ExportNsiItemResult getNsiItem(String grp, BigInteger id) throws Fault, CantSignSoap, CantSendSoap {
		// выполнить инициализацию
		setUp();

		ExportNsiItemRequest req = new ExportNsiItemRequest();
	    req.setListGroup(grp);
	    req.setRegistryNumber(id);
		//req.setId("foo");
		req.setVersion(req.getVersion()==null?reqProp.getGisVersion():req.getVersion());

		ExportNsiItemResult ex = port.exportNsiItem(req);

		// освободить ресурсы
		//sb.closeResource();
	   return ex;
	}
*/
}
