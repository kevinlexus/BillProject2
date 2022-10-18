package com.dic.app.service.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.service.ConfigApp;
import com.dic.app.service.DebitByLskThrMng;
import com.dic.app.service.GenPenProcessMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.CalcStoreLocal;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Ko;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.time.ZonedDateTime;
import java.util.Date;

/**
 * Сервис формирования задолженностей и пени
 *
 * @author lev
 * @version 1.0
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class GenPenProcessMngImpl implements GenPenProcessMng {

    private final ChargeDAO chargeDao;
    private final ChangeDAO changeDao;
    private final KwtpMgDAO kwtpMgDao;
    private final CorrectPayDAO correctPayDao;
    private final ChargePayDAO chargePayDAO;
    private final DebitByLskThrMng debitByLskThrMng;
    private final ConfigApp configApp;

    @PersistenceContext
    private EntityManager em;


    /**
     * Рассчет задолженности и пени по всем лиц.счетам помещения
     *
     * @param reqConf   запрос
     * @param isCalcPen рассчитывать пеню?
     * @param klskId    klskId помещения
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRES_NEW,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    public void genDebitPen(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen {
        Ko ko = em.find(Ko.class, klskId);
        for (Kart kart : ko.getKart()) {
            genDebitPenKart(reqConf, kart);
        }
        ko.setDtGenDebPen(Date.from(ZonedDateTime.now().toInstant()));
    }

    /**
     * Рассчет задолженности и пени по всем лиц.счетам помещения для тестов - без создания новой транзакции
     *
     * @param calcStore - хранилище объемов, справочников
     * @param isCalcPen - рассчитывать пеню?
     * @param klskId    - klskId помещения
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    public void genDebitPenForTest(RequestConfigDirect reqConf, boolean isCalcPen, long klskId) throws ErrorWhileChrgPen {
        Ko ko = em.find(Ko.class, klskId);
        for (Kart kart : ko.getKart()) {
            genDebitPenKart(reqConf, kart);
        }
    }

    /**
     * Рассчет задолженности и пени по лиц.счету
     *
     * @param reqConf запрос
     * @param kart    лиц.счет
     */
    private void genDebitPenKart(RequestConfigDirect reqConf, Kart kart) throws ErrorWhileChrgPen {
        Integer period = Integer.parseInt(configApp.getPeriod());
        Integer periodBack = Integer.parseInt(configApp.getPeriodBack());

        // сформировать движение по лиц.счету (для пени - не нужно, сделал сюда, чтобы выполнялось многопоточно)
        chargePayDAO.genChrgPay(kart.getLsk(), 0, reqConf.getGenDt());
        // загрузить все финансовые операции по лиц.счету
        CalcStoreLocal localStore = new CalcStoreLocal();
        // задолженность предыдущего периода
        //if (calcStore.getMapDebit().size() > 0) {
        //    localStore.setLstDebFlow(calcStore.getMapDebit().get(kart.getLsk()));
        //} else {
        localStore.setLstDebFlow(chargePayDAO.getDebitByLsk(kart.getLsk(), periodBack));

        // текущее начисление - 2
        localStore.setChrgSum(chargeDao.getChargeByPeriodAndLsk(kart.getLsk()));
        // перерасчеты - 5
        localStore.setLstChngFlow(changeDao.getChangeByPeriodAndLsk(kart.getLsk()));
        // оплата - 3
        localStore.setLstPayFlow(kwtpMgDao.getKwtpMgByPeriodAndLsk(kart.getLsk()));
        // корректировки оплаты - 6
        localStore.setLstPayCorrFlow(correctPayDao.getCorrectByPeriodAndLsk(kart.getLsk(), String.valueOf(period)));

        // преобразовать String код reu в int, для ускорения фильтров
        localStore.setReuId(Integer.parseInt(kart.getUk().getReu()));

        debitByLskThrMng.genDebPen(kart, reqConf, localStore);
    }

}