package com.dic.app.gis.service.st.impl;

import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.Org;
import com.ric.cmn.excp.CantPrepSoap;

public interface ReqProps {

    void setPropBefore(Task task) throws CantPrepSoap;
    void setPropBeforeSimple(Task task) throws CantPrepSoap;

    void setPropAfter(Task task);

    void setPropWOGUID(Task task, SoapBuilder sb) throws CantPrepSoap;

    Task getFoundTask();

    String getHouseGuid();

    String getKul();

    String getNd();

    Org getUk();

    String getGisVersion();

    String getPpGuid();

    String getHostIp();


    int getSignerId();
}