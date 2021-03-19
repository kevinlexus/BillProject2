package com.dic.bill.dto;

import java.math.BigDecimal;

/*
 * Projection для хранения записи финансовой операции (входящии записи по долгу) по лиц счету
 * @author - Lev
 * @ver 1.00
 */
public interface SumDebPenLskRec extends SumDebPenRec {
	// лиц.счет
	String getLsk();
}
