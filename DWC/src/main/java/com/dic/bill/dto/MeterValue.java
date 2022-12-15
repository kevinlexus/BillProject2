package com.dic.bill.dto;

import java.math.BigDecimal;
import java.util.Date;

/*
 * Projection для хранения записи показания счетчиков
 */
public interface MeterValue {

    Integer getId(); // id записи t_objxpar
	BigDecimal getN1(); // показание счетчика
	Date getDtCrt();  // создано
	String getGuid(); // GUID счетчика
	Integer getEolinkId(); // eolink.id счетчика
}
