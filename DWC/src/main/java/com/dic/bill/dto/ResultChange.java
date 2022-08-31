package com.dic.bill.dto;

import com.dic.bill.enums.ChangeTps;
import lombok.Builder;
import lombok.Value;

import java.math.BigDecimal;

@Value
@Builder
public class ResultChange {
    String lsk;
    String mg;
    String uslId;
    Integer orgId;
    BigDecimal proc;
    BigDecimal summa;
    BigDecimal vol;
    Integer cntDays;
    ChangeTps tp;
}
