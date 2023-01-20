package com.dic.bill.dto;

import java.math.BigDecimal;
import java.util.Date;

/*
 * Projection для хранения записи перерасчета
 * @author - Lev
 * @ver 1.00
 */
public interface SumChangeRec {

	String getStreet(); // улица
	String getNd(); // № дома
	String getNm(); // услуга
	BigDecimal getSumma(); // сумма
}
