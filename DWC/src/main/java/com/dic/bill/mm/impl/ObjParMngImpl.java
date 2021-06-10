package com.dic.bill.mm.impl;

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
import com.ric.dto.MapKoAddress;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.math.BigDecimal;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class ObjParMngImpl implements ObjParMng {

    private final UlstDAO ulstDAO;
    private final ObjParDAO objParDAO;

    private final KartMng kartMng;

    public ObjParMngImpl(UlstDAO ulstDAO, ObjParDAO objParDAO, KartMng kartMng) {
        this.ulstDAO = ulstDAO;
        this.objParDAO = objParDAO;
        this.kartMng = kartMng;
    }

    /**
     * Получить значение параметра типа BigDecimal объекта по CD свойства
     *
     * @param ko - Объект Ko
     * @param cd - CD параметра
     * @return
     */
    @Override
    public BigDecimal getBd(Ko ko, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("NM")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом NM!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(ko.getId(), cd);
        if (objPar != null) {
            return objPar.getN1();
        } else {
            return null;
        }
    }

    /**
     * Получить значение параметра типа String объекта по CD свойства
     *
     * @param ko - Объект Ko
     * @param cd - CD параметра
     * @return
     */
    @Override
    public String getStr(Ko ko, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("ST")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом NM!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(ko.getId(), cd);
        if (objPar != null) {
            return objPar.getS1();
        } else {
            return null;
        }
    }

    /**
     * Получить значение параметра типа Date объекта по CD свойства
     *
     * @param ko - Объект Ko
     * @param cd - CD параметра
     * @return
     */
    @Override
    public Date getDate(Ko ko, String cd) throws WrongParam, WrongGetMethod {
        Lst lst = ulstDAO.getByCd(cd);
        if (lst == null) {
            throw new WrongParam("ОШИБКА! Несуществующий параметр CD=" + cd);
        } else if (!lst.getValTp().equals("DT")) {
            throw new WrongGetMethod("ОШИБКА! Попытка получить значение параметра " + cd + " не являющегося типом NM!");
        }

        ObjPar objPar = objParDAO.getByKlskCd(ko.getId(), cd);
        if (objPar != null) {
            return objPar.getD1();
        } else {
            return null;
        }
    }

    @Override
    public MapKoAddress getMapKoAddressByObjPar(String cd, Long userId) {
        Map<Long, KoAddress> mapAddress = new HashMap<>();
        final int[] ord = {1};
        objParDAO.getKoByObjPar(cd, new BigDecimal(userId)).stream()
                .flatMap(t -> t.getKart().stream().filter(Kart::isActual))
                .forEach(d ->
                        mapAddress.put(d.getKoKw().getId(),
                                new KoAddress(ord[0]++, d.getKoKw().getId(), kartMng.getAdrWithCity(d)))
                );
        return new MapKoAddress(mapAddress);
    }
}