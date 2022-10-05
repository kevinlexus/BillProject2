package com.dic.app.service;

import java.math.BigDecimal;
import java.util.List;

import com.dic.app.service.impl.Cnt;
import com.dic.bill.dto.SumDebMgRec;
import com.dic.bill.dto.SumDebUslMgRec;

public interface MigrateUtlMng {

	void printChrg(List<SumDebUslMgRec> lstChrg);
	void printDeb(List<SumDebMgRec> lstDeb);
	void check(List<SumDebUslMgRec> lstSal, List<SumDebMgRec> lstDeb, Cnt cnt);
	void distDebFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt, String lsk);
	void distSalFinal(Integer period, List<SumDebUslMgRec> lstDebResult, Cnt cnt);
	List<SumDebMgRec> getDeb(String lsk, Integer period);
	int getDebTp(List<SumDebMgRec> lstDeb);
	void setWeigths(List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg, int sign);
	List<SumDebUslMgRec> getChrg(String lsk, List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal);
	List<SumDebUslMgRec> getSal(String lsk, Integer period);
	List<SumDebUslMgRec> getPeriod(String uslId, Integer orgId, List<SumDebUslMgRec> lstChrg);
	void insDebResult(List<SumDebUslMgRec> lstDebResult, Integer period, String uslId, Integer orgId, BigDecimal summa,
			int sign);
	void printDebResult(List<SumDebUslMgRec> lstDebResult);
	void printSal(List<SumDebUslMgRec> lstSal);
	void checkSumma(List<SumDebUslMgRec> lstSal, List<SumDebMgRec> lstDeb, String lsk);
	void addSurrogateChrg(List<SumDebMgRec> lstDeb, List<SumDebUslMgRec> lstSal, List<SumDebUslMgRec> lstChrg,
			int sign);

}
