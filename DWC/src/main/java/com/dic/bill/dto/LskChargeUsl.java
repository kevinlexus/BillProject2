package com.dic.bill.dto;

import com.dic.bill.model.scott.Charge;
import com.dic.bill.model.scott.Usl;
import lombok.Builder;
import lombok.Data;
import lombok.Value;

import java.math.BigDecimal;

@Builder
@Data
public class LskChargeUsl {
    Long kLskId;
    String lsk;
    String uslId;
    Integer orgId;
    BigDecimal vol;
    BigDecimal summa;
    // структуры, для округления в ГИС
    BigDecimal price;
    BigDecimal area;
    Charge charge;
}
