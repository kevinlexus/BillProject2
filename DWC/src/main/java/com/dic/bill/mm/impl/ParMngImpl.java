package com.dic.bill.mm.impl;

import com.dic.bill.dao.ParDAO;
import com.dic.bill.mm.ParMng;
import com.dic.bill.model.bs.Par;
import com.dic.bill.model.scott.Param;
import com.ric.cmn.Utl;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.util.Date;
import java.util.Map;

//import com.dic.bill.model.fn.Chng;
//import com.ric.bill.model.fn.ChngVal;


@Service
@Slf4j
public class ParMngImpl implements ParMng {

    private final ParDAO pDao;
    @Autowired
    private EntityManager em;

    public ParMngImpl(ParDAO pDao) {
        this.pDao = pDao;
    }

    //получить параметр по его CD
    public Par getByCD(int rqn, String cd) {
        return pDao.getByCd(rqn, cd);
    }

    /**
     * Узнать существует ли параметр по его CD
     */
    @Cacheable(cacheNames = "ParMngImpl.isExByCd", key = "{#rqn, #cd }", unless = "#result == null")
    public boolean isExByCd(int rqn, String cd) {
        Par p = getByCD(rqn, cd);
        if (p != null) {
            return true;
        } else {
            return false;
        }
    }

    @Transactional
    public void reloadParam(Map<String, Date> mapDate, Map<String, Boolean> mapParams) {
        Param param = em.find(Param.class, 1);
        em.refresh(param);
        Date dtFirst;
        try {
            dtFirst = Utl.getDateFromPeriod(param.getPeriod());
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            throw new RuntimeException("Ошибка при загрузке справочника Params");
        }
        LocalDate localDtFirst = dtFirst.toInstant().atZone(ZoneId.systemDefault()).toLocalDate();
        Date dtLast = Date.from(localDtFirst.withDayOfMonth(localDtFirst.lengthOfMonth()).atStartOfDay(ZoneId.systemDefault()).toInstant());
        LocalDate localDtMiddle = localDtFirst.withDayOfMonth(15);
        Date dtMiddle = Date.from(localDtMiddle.atStartOfDay(ZoneId.systemDefault()).toInstant());
        mapDate.put("dtFirst", dtFirst);
        mapDate.put("dtLast", dtLast);
        mapDate.put("dtMiddle", dtMiddle);
        log.info("Загружены даты текущего периода начало={}, окончание={}, середина={}",
                Utl.getStrFromDate(dtFirst, "dd.MM.yyyy"),
                Utl.getStrFromDate(dtLast, "dd.MM.yyyy"),
                Utl.getStrFromDate(dtMiddle, "dd.MM.yyyy"));
        log.info("******* 2 Загружены даты текущего периода начало={}, окончание={}, середина={}",
                mapDate.get("dtFirst"),
                mapDate.get("dtLast"),
                mapDate.get("dtMiddle"));
        mapParams.put("isDetChrg", param.getIsDetChrg());
    }
}
