package com.dic.bill.dto;

import com.dic.bill.model.scott.Usl;
import lombok.Value;

import java.math.BigDecimal;
import java.util.List;
import java.util.Map;

@Value
public class UslMeterVol {
    List<UslMeterDateVol> lstDayMeterVol;
    // итоговый объем по услугам, за месяц
    Map<Usl, BigDecimal> amountVol;
}
