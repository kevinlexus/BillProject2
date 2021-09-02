package com.dic.bill.model.scott;

import java.math.BigDecimal;

public interface Nabors {

    boolean isActive(boolean isForVol);
    Usl getUsl();
    BigDecimal getKoeff();
    BigDecimal getNorm();

}
