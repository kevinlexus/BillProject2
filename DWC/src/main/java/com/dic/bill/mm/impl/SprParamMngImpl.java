package com.dic.bill.mm.impl;

import com.dic.bill.dao.SprParamDAO;
import com.dic.bill.dao.SprPenDAO;
import com.dic.bill.dao.StavrDAO;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.model.scott.SprParam;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.ric.cmn.excp.WrongParam;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.jpa.repository.QueryHints;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.QueryHint;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Сервис обслуживания справочника параметров
 * @version 1.0
 */
@Service
@Slf4j
public class SprParamMngImpl implements SprParamMng {

    private SprParamDAO sprParamDao;
	private final SprPenDAO sprPenDAO;
	private final StavrDAO stavrDAO;

	public SprParamMngImpl(SprParamDAO sprParamDao, SprPenDAO sprPenDAO, StavrDAO stavrDAO) {
		this.sprParamDao = sprParamDao;
		this.sprPenDAO = sprPenDAO;
		this.stavrDAO = stavrDAO;
	}

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

	@Value
	public static class StavPen {
		// справочник дат начала пени
		List<SprPen> lstSprPen;
		// справочник ставок рефинансирования
		List<Stavr> lstStavr;
		// справочник дат начала пени
		Map<SprPenKey, SprPen> mapSprPen;
	}

	/**
	 * Перезагрузить справочники пени
	 * @return DTO кранящий оба справочника
	 */
	@Override
	@Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
	public StavPen getStavPen() {
		List<SprPen> lstSprPen = sprPenDAO.findAll();
		Map<SprPenKey, SprPen> mapSprPen = new HashMap<>();
		lstSprPen.forEach(t -> mapSprPen.put(new SprPenKey(t.getTp(), t.getMg(), t.getReu()), t));
		log.info("Загружен справочник дат начала обязательства по оплате");
		List<Stavr> lstStavr = stavrDAO.findAll();
		log.info("Загружен справочник ставок рефинансирования");
		return new StavPen(lstSprPen, lstStavr, mapSprPen);
	}


}