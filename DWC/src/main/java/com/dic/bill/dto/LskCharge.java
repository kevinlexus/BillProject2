package com.dic.bill.dto;

import java.math.BigDecimal;

public interface LskCharge {

    Long getKlskId();

    String getLsk();

    String getMg();

    String getUslId();

    Integer getOrgId();

    BigDecimal getSumma();

    BigDecimal getVol();
}
