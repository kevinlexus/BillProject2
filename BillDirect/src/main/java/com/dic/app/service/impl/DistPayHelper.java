package com.dic.app.service.impl;

import com.dic.app.enums.BaseForDistPays;
import com.dic.app.service.ConfigApp;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dto.Amount;
import com.dic.bill.dto.SumUslOrgDTO;
import com.dic.bill.mm.SaldoMng;
import com.dic.bill.model.scott.KwtpDayLog;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import javax.persistence.EntityManager;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class DistPayHelper {

    private final EntityManager em;
    private final SaldoMng saldoMng;
    private final ConfigApp configApp;
    private final SaldoUslDAO saldoUslDAO;

    public List<SumUslOrgDTO> getBaseForDistrib(Amount amount, BaseForDistPays tp, boolean isIncludeByClosedOrgList,
                                                boolean isExcludeByClosedOrgList, List<String> lstExcludeUslId,
                                                List<String> lstFilterByUslId, String currPeriod) throws WrongParam {
        List<SumUslOrgDTO> lstDistribBase;
        if (tp.equals(BaseForDistPays.IN_SAL_0)) {
            // получить вх.сал.
            lstDistribBase = new ArrayList<>(amount.getInSal());
        } else if (tp.equals(BaseForDistPays.CURR_CHARGE_1)) {
            // получить текущее начисление
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, true, false, false,
                    false, false, null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp.equals(BaseForDistPays.BACK_PERIOD_CHARGE_3)) {
            // получить начисление предыдущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, false, true, false, false, false,
                    configApp.getPeriodBack(), false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp.equals(BaseForDistPays.ALREADY_DISTRIB_PAY_4)) {
            // получить уже распределенную сумму оплаты, в качестве базы для распределения (обычно распр.пени)
            lstDistribBase = amount.getLstDistPayment();
        } else if (tp.equals(BaseForDistPays.CURR_PERIOD_CHARGE_5)) {
            // получить начисление текущего периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    null, null,
                    false, true, false, false, false, false,
                    null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp.equals(BaseForDistPays.IN_SAL_PEN_6)) {
            // получить вх.сальдо по пене
            lstDistribBase = saldoUslDAO.getPinSalXitog3ByLsk(amount.getKart().getLsk(), currPeriod)
                    .stream().map(t -> new SumUslOrgDTO(t.getUslId(), t.getOrgId(), t.getSumma()))
                    .collect(Collectors.toList());
        } else if (tp.equals(BaseForDistPays.CURR_PERIOD_CHARGE_MOIFY_7)) {
            // получить текущее начисление ред. 25.06.2019 убрал вот это:минус оплата за текущий период и корректировки по оплате за текущий период
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    null, null,
                    false, true, false, false, false,
                    false, null, false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else if (tp.equals(BaseForDistPays.SELECTED_PERIOD_CHARGE_8)) {
            // получить начисление выбранного периода
            List<SumUslOrgDTO> inSal = saldoMng.getOutSal(amount.getKart(), currPeriod,
                    amount.getLstDistPayment(), amount.getLstDistPayment(),
                    false, false, true, false, false, false,
                    amount.getDopl(), false, false);
            // фильтровать по положительным значениям
            lstDistribBase = inSal.stream()
                    .filter(t -> t.getSumma().compareTo(BigDecimal.ZERO) > 0
                    ).collect(Collectors.toList());
        } else {
            throw new WrongParam("Некорректный параметр tp=" + tp);
        }
        // исключить нули
        lstDistribBase.removeIf(t -> t.getSumma().compareTo(BigDecimal.ZERO) == 0);

        // исключить услуги
        if (lstExcludeUslId != null) {
            lstDistribBase.removeIf(t -> lstExcludeUslId.contains(t.getUslId()));
        }
        // оставить только услуги по списку
        if (lstFilterByUslId != null) {
            lstDistribBase.removeIf(t -> !lstFilterByUslId.contains(t.getUslId()));
        }

        if (isIncludeByClosedOrgList) {
            // оставить только услуги и организации, содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().noneMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getDopl(), d.getMgFrom(), d.getMgTo()) // период
                    )
            );
        } else if (isExcludeByClosedOrgList) {
            // оставить только услуги и организации, НЕ содержащиеся в списке закрытых орг.
            lstDistribBase.removeIf(t -> amount.getLstSprProcPay()
                    .stream().anyMatch(d -> amount.getKart().getUk().equals(d.getUk()) // УК
                            && t.getUslId().equals(d.getUsl().getId())  // услуга
                            && t.getOrgId().equals(d.getOrg().getId())  // организация - поставщик
                            && Utl.between2(amount.getDopl(), d.getMgFrom(), d.getMgTo()) // период
                    )
            );
        } else
            //noinspection ConstantConditions
            if (isIncludeByClosedOrgList && isExcludeByClosedOrgList) {
                throw new WrongParam("Некорректно использовать isIncludeByClosedOrgList=true и " +
                        "isExcludeByClosedOrgList=true одновременно!");
            }

        BigDecimal amntSal = lstDistribBase.stream()
                .map(SumUslOrgDTO::getSumma)
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        if (tp.equals(BaseForDistPays.CURR_CHARGE_1) || tp.equals(BaseForDistPays.CURR_PERIOD_CHARGE_MOIFY_7)) {
            saveKwtpDayLog(amount, "Выбранное текущее начисление > 0 для распределения оплаты, по лиц.счету lsk={}:",
                    amount.getKart().getLsk());
        }
        saveKwtpDayLog(amount, "Будет распределено по строкам:");
        lstDistribBase.forEach(t ->
                saveKwtpDayLog(amount, "usl={}, org={}, summa={}",
                        t.getUslId(), t.getOrgId(), t.getSumma()));
        saveKwtpDayLog(amount, "итого:{}", amntSal);
        return lstDistribBase;
    }


    /**
     * Сохранить сообщение в лог KWTP_DAY_LOG
     *
     * @param amount - итоги распределения
     * @param msg    - сообщение
     * @param t      - параметры
     */
    public void saveKwtpDayLog(Amount amount, String msg, Object... t) {
        int kwtpMgId = amount.getKwtpMgId();
        KwtpDayLog kwtpDayLog =
                KwtpDayLog.KwtpDayLogBuilder.aKwtpDayLog()
                        .withNpp(amount.getNpp())
                        .withFkKwtpMg(kwtpMgId)
                        .withText(Utl.getStrUsingTemplate(msg, t)).build();
        amount.setNpp(amount.getNpp() + 1);
        em.persist(kwtpDayLog); // note Используй crud.save
        log.info(msg, t);
    }

}
