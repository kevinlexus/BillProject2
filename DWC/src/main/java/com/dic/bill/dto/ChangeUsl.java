package com.dic.bill.dto;

import lombok.Value;

import java.math.BigDecimal;

@Value
public class ChangeUsl {
    String uslId;
    Integer org1Id;
    BigDecimal proc1;
    Integer org2Id;
    BigDecimal proc2;
    BigDecimal absSet;
    Integer cntDays;
    Integer cntDays2;
}
