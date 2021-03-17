package com.dic.bill.mm.impl;

import com.dic.bill.dao.SprParamDAO;
import com.dic.bill.dao.TaskDAO;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.mm.TaskMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.SprParam;
import com.dic.bill.model.scott.Vvod;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.QueryHint;
import java.util.Date;
import java.util.List;

/**
 * Сервис обслуживания справочника параметров
 * @version 1.0
 */
@Service
@Slf4j
public class SprParamMngImpl implements SprParamMng {

	@Autowired
    private SprParamDAO sprParamDao;

	/**
	 * Получить параметр типа Double
	 * @param cd - CD параметра
	 * @return
	 */
	@Override
	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	public Double getN1(String cd) throws WrongParam {
		SprParam par = sprParamDao.getByCD(cd, 0);
		if (par !=null) {
			return par.getN1();
		} else {
			throw new WrongParam("Несуществующий параметр в справочнике SPR_PARAMS: CD="+cd+" cdTp="+0);
		}
	}

	/**
	 * Получить параметр типа String
	 * @param cd - CD параметра
	 * @return
	 */
	@Override
	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	public String getS1(String cd) throws WrongParam {
		SprParam par = sprParamDao.getByCD(cd, 1);
		if (par !=null) {
			return par.getS1();
		} else {
			throw new WrongParam("Несуществующий параметр в справочнике SPR_PARAMS: CD="+cd+" cdTp="+1);
		}
	}

	/**
	 * Получить параметр типа Date
	 * @param cd - CD параметра
	 * @return
	 */
	@Override
	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	public Date getD1(String cd) throws WrongParam {
		SprParam par = sprParamDao.getByCD(cd, 2);
		if (par !=null) {
			return par.getD1();
		} else {
			throw new WrongParam("Несуществующий параметр в справочнике SPR_PARAMS: CD="+cd+" cdTp="+2);
		}
	}

	/**
	 * Получить параметр типа Boolean
	 * @param cd - CD параметра
	 * @return
	 */
	@Override
	@QueryHints(value = { @QueryHint(name = org.hibernate.annotations.QueryHints.FLUSH_MODE, value = "COMMIT") })
	public Boolean getBool(String cd) throws WrongParam {
		SprParam par = sprParamDao.getByCD(cd, 0);
		if (par !=null) {
			if (par.getN1()==null) {
				return null;
			} else if (par.getN1().equals(1D)) {
				return true;
			} else {
				return false;
			}
		} else {
			throw new WrongParam("Несуществующий параметр в справочнике SPR_PARAMS: CD="+cd+" cdTp="+0);
		}
	}
}