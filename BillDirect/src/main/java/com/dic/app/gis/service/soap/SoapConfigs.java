package com.dic.app.gis.service.soap;

import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.sec.User;
import com.ric.cmn.excp.UnusableCode;

public interface SoapConfigs {

    String getOrgPPGuid();

    String getHostIp();

    String getPrefixGis();

    Eolink getRkcByHouse(Eolink eolink);

    String getSignPass();

    String getSignPath();

    String getSignPass2();

    String getSignPath2();

    void saveError(Eolink eolink, long err, boolean isSet) throws UnusableCode;

    boolean isGisKeysLoaded();
}
