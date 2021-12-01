package com.dic.app.mm;

import com.dic.bill.Lock;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.model.scott.Org;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.dic.bill.model.scott.Tuser;
import com.dic.bill.model.sec.User;

import java.text.ParseException;
import java.util.*;

public interface ConfigApp {

    Integer getProgress();

    String getPeriod();

    Tuser getCurUser();

    Optional<User> getCurUserGis();

    String getPeriodNext();

    String getPeriodBack();

    Date getCurDt1();

    Date getCurDt2();

    Lock getLock();

    Date getDtMiddleMonth();

    int incNextReqNum();

    void setProgress(Integer progress);

    void incProgress();

    Map<SprPenKey, SprPen> getMapSprPen();

    Map<String, Boolean> getMapParams();

    List<Stavr> getLstStavr();

    void reloadSprPen();

    void reloadParam() throws ParseException;

    Set<String> getWaterUslCodes();

    Set<String> getWasteUslCodes();

    Set<String> getWaterOdnUslCodes();

    Set<String> getWasteOdnUslCodes();

    Map<String, Set<String>> getMapUslRound();

    Map<String, Org> getMapReuOrg();

    String getGisVersion();

    String getHostIp();


}