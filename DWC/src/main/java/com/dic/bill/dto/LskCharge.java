package com.dic.bill.dto;

import java.math.BigDecimal;

public interface LskCharge {

    Long getKlskId();

    String getLsk();

    String getMg();

    String getUslId();

    // если присутствует (not null) использовать его, вместо naborOrgId, так как формируется начислением с разбиением nabor по dt1, dt2
    Integer getChrgOrgId();

    // использовать, если сhrgOrgId == null (для старых записей архива)
    Integer getNaborOrgId();

    BigDecimal getSumma();

    BigDecimal getVol();
}
