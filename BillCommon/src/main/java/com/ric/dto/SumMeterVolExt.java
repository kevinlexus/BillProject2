package com.ric.dto;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.Value;

import java.math.BigDecimal;
import java.util.Date;

@Value
@AllArgsConstructor()
public class SumMeterVolExt {

    Integer meterId;
    String uslId;
    Integer npp;
    Integer serviceNpp;
    Date dtFrom;
    Date dtTo;
    BigDecimal vol;
    BigDecimal n1;
    String serviceName;

}
