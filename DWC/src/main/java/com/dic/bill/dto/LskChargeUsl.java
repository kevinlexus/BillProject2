package com.dic.bill.dto;

import com.dic.bill.model.scott.Charge;
import lombok.Builder;
import lombok.Data;

import java.math.BigDecimal;

@Builder
@Data
public class LskChargeUsl implements LskCharge {
    Long klskId;
    String lsk;
    String uslId;
    Integer orgId;
    BigDecimal vol;
    BigDecimal summa;
    // структуры, для округления в ГИС
    BigDecimal price;
    BigDecimal area;
    Charge charge;
    // для совместимости с LskCharge:
    String mg;
    Integer naborOrgId;


}
