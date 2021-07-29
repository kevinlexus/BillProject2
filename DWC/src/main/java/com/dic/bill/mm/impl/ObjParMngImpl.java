package com.dic.bill.mm.impl;

import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.KoDAO;
import com.dic.bill.dao.ObjParDAO;
import com.dic.bill.dao.UlstDAO;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.ObjParMng;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.dic.bill.model.scott.Lst;
import com.dic.bill.model.scott.ObjPar;
import com.ric.cmn.excp.WrongGetMethod;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.KoAddress;
import com.ric.dto.ListKoAddress;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
@RequiredArgsConstructor
public class ObjParMngImpl implements ObjParMng {

    private final UlstDAO ulstDAO;
    private final ObjParDAO objParDAO;
    private final KartMng kartMng;
    private final KoDAO koDAO;
    private final EntityManager em;

    /**
     * Получить значение параметра типа BigDecimal объекта по CD свойства
     *
     * @param klskId KlskId объекта
     * @param cd     CD параметра
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public BigDecimal getBd(long klskId, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("NM")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом Number!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(klskId, cd);
        if (objPar != null) {
            return objPar.getN1();
        } else {
            return null;
        }
    }

    /**
     * Получить значение параметра типа String объекта по CD свойства
     *
     * @param klskId KlskId объекта
     * @param cd     CD параметра
     */
    @Override
    @Transactional(propagation = Propagation.MANDATORY, rollbackFor = Exception.class)
    public String getStr(long klskId, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("ST")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом String!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(klskId, cd);
        if (objPar != null) {
            return objPar.getS1();
        } else {
            return null;
        }
    }

    /**
     * Получить значение параметра типа Boolean объекта по CD свойства
     *
     * @param klskId KlskId объекта
     * @param cd     CD параметра
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public Boolean getBool(long klskId, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("BL")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом Boolean!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(klskId, cd);
        if (objPar != null) {
            return objPar.getN1().compareTo(BigDecimal.ONE) == 0;
        } else {
            return null;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void setBool(long klskId, String cd, boolean val) throws WrongParam, WrongGetMethod {
        setBoolValue(klskId, cd, val);
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void setBoolNewTransaction(long klskId, String cd, boolean val) throws WrongParam, WrongGetMethod {
        setBoolValue(klskId, cd, val);
    }

    private void setBoolValue(long klskId, String cd, boolean val) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("BL")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом Boolean!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(klskId, cd);
        if (objPar == null) {
            objPar = new ObjPar();
            objPar.setStatus(0);
            objPar.setKo(koDAO.getByKlsk(klskId));
            objPar.setLst(lst);
        }
        if (val) {
            objPar.setN1(BigDecimal.ONE);
        } else {
            objPar.setN1(BigDecimal.ZERO);
        }
        objParDAO.save(objPar);
    }

    /**
     * Получить значение параметра типа Date объекта по CD свойства
     *
     * @param klskId KlskId объекта
     * @param cd     CD параметра
     */
    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public Date getDate(long klskId, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("DT")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом Date!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(klskId, cd);
        if (objPar != null) {
            return objPar.getD1();
        } else {
            return null;
        }
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public ListKoAddress getListKoAddressByObjPar(String cd, Long userId) {
        Map<Long, KoAddress> addressMap = new HashMap<>();
        final int[] ord = {1};
        objParDAO.getKoByObjPar(cd, new BigDecimal(userId)).stream()
                .flatMap(t -> t.getKart().stream().filter(Kart::isActual))
                .forEach(d -> addressMap.putIfAbsent(d.getKoKw().getId(),
                        new KoAddress(ord[0]++, d.getKoKw().getId(), kartMng.getAdrWithCity(d))));
        return new ListKoAddress(new ArrayList<>(addressMap.values()));
    }
}