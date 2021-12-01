package com.dic.app.gis.service.soap;

import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.sec.User;
import com.ric.cmn.excp.UnusableCode;

public interface SoapConfigs {

    String getOrgPPGuid();

    String getHostIp();

/*
    String getFingerPrint();

    String getBscLogin();

    String getBscPass();
*/

    String getPrefixGis();

    //User getCurUser();

    Eolink getRkcByHouse(Eolink eolink);

    String getSignPass();

    String getSignPath();

    String getSignPass2();

    String getSignPath2();

    void saveError(Eolink eolink, long err, boolean isSet) throws UnusableCode;
}
