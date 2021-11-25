package com.dic.app.gis.service.soapbuilders;

import java.math.BigInteger;

import ru.gosuslugi.dom.schema.integration.nsi_base.NsiItemType;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiListType;

import com.ric.cmn.excp.CantSendSoap;
import com.ric.cmn.excp.CantSignSoap;
import ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault;

public interface NsiCommonAsyncBindingBuilders {

	public NsiListType getNsiList(String tp) throws Fault, CantSignSoap, CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault;
	public NsiItemType getNsiItem(String TypeList,BigInteger TypeItem) throws Fault, CantSignSoap, CantSendSoap, ru.gosuslugi.dom.schema.integration.nsi_common_service_async.Fault;

}
