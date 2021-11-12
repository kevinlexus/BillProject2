package com.dic.app.mm.impl;

import com.dic.app.mm.ChangeMng;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.dto.ChangeUsl;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskCharge;
import com.dic.bill.dto.ResultChange;
import com.ric.cmn.Utl;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class ChangeMngImpl implements ChangeMng {

    private final ConfigApp configApp;

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public List<ResultChange> genChanges(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskCharge>>> chargeByPeriod) {
        log.info("klskId = {}", klskId);
        List<ResultChange> resultChanges = new ArrayList<>();
        LocalDate dtFrom = LocalDate.of(Integer.parseInt(changesParam.getPeriodFrom().substring(0, 4)),
                Integer.parseInt(changesParam.getPeriodFrom().substring(4)), 1);
        LocalDate dtTo = LocalDate.of(Integer.parseInt(changesParam.getPeriodTo().substring(0, 4)),
                Integer.parseInt(changesParam.getPeriodTo().substring(4)), 1);
        // перебрать заданные периоды
        for (LocalDate dt = dtFrom; dt.isBefore(dtTo.plusDays(1)); dt = dt.plusMonths(1)) {
            String period = Utl.getPeriodFromDate(dt);
            Map<String, List<LskCharge>> chargesByUsl = chargeByPeriod.get(period);
            if (chargesByUsl != null) {
                // объем по услугам водоотведения
                List<LskCharge> chargeWasteList = chargesByUsl.entrySet().stream()
                        .filter(t -> configApp.getWasteUslCodes().contains(t.getKey()))
                        .flatMap(t -> t.getValue().stream())
                        .collect(Collectors.toList());
                // объем по услугам водоотведения (ОДН)
                List<LskCharge> chargeWasteOdnList = chargesByUsl.entrySet().stream()
                        .filter(t -> configApp.getWasteOdnUslCodes().contains(t.getKey()))
                        .flatMap(t -> t.getValue().stream())
                        .collect(Collectors.toList());
                for (ChangeUsl changeUsl : changesParam.getChangeUslList()) {
                    // перебрать заданные в перерасчете услуги
                    if (changeUsl.getProc1().compareTo(BigDecimal.ZERO) != 0) {
                        List<LskCharge> charges = chargesByUsl.get(changeUsl.getUslId());
                        if (charges != null) {
                            for (LskCharge lskCharge : charges) {
                                // перебрать начисление по услуге (может идти несколькими строками, разбито по орг)
                                if (lskCharge.getSumma().compareTo(BigDecimal.ZERO) != 0) {
                                    BigDecimal sumChange = lskCharge.getSumma()
                                            .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                                            .multiply(changeUsl.getProc1()).setScale(2, RoundingMode.HALF_UP);
                                    Integer orgId = getChangeOrgId(changeUsl, lskCharge);
                                    if (orgId == null) {
                                        log.error("Значение orgId должно быть либо задано пользователем, либо получено из начисления за период, или из наборов услуг");
                                        throw new RuntimeException("Пустое значение orgId по лиц.счету:" + lskCharge.getLsk());
                                    }
                                    ResultChange resultChange = ResultChange.builder()
                                            .lsk(lskCharge.getLsk())
                                            .mg(lskCharge.getMg())
                                            .uslId(changeUsl.getUslId())
                                            .org1Id(orgId)
                                            .proc1(changeUsl.getProc1())
                                            .summa(sumChange).build();
                                    resultChanges.add(resultChange);
                                    if (changesParam.getIsAddUslWaste()) {
                                        // добавить перерасчет по водоотведению или водоотведению ОДН
                                        if (configApp.getWaterUslCodes().contains(changeUsl.getUslId())) {
                                            resultChanges.addAll(getWastedChangesResult(chargeWasteList, changeUsl, lskCharge));
                                        } else if (configApp.getWaterOdnUslCodes().contains(changeUsl.getUslId())) {
                                            resultChanges.addAll(getWastedChangesResult(chargeWasteOdnList, changeUsl, lskCharge));
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
        return resultChanges;
    }

    private List<ResultChange> getWastedChangesResult(List<LskCharge> chargeWasteList, ChangeUsl changeUsl, LskCharge lskCharge) {
        List<ResultChange> resultChanges = new ArrayList<>();
        for (LskCharge lskChargeWaste : chargeWasteList) {
            if (lskChargeWaste.getVol() != null && lskChargeWaste.getVol().compareTo(BigDecimal.ZERO) != 0) {
                BigDecimal procWaste = lskCharge.getVol()
                        .divide(lskChargeWaste.getVol(), 10, RoundingMode.HALF_UP)
                        .multiply(changeUsl.getProc1()).setScale(2, RoundingMode.HALF_UP);
                if (procWaste.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal sumChange = lskChargeWaste.getSumma()
                            .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                            .multiply(procWaste).setScale(2, RoundingMode.HALF_UP);
                    Integer orgId = getChangeOrgId(changeUsl, lskChargeWaste);
                    ResultChange resultChange = ResultChange.builder()
                            .lsk(lskChargeWaste.getLsk())
                            .mg(lskCharge.getMg())
                            .uslId(lskChargeWaste.getUslId())
                            .org1Id(orgId)
                            .proc1(procWaste)
                            .summa(sumChange).build();
                    resultChanges.add(resultChange);
                }
            }
        }
        return resultChanges;
    }

    /**
     * Получить организацию для перерасчета
     *
     * @param changeUsl параметры перерасчета от пользователя
     * @param lskCharge текущая строка начисления
     */
    private Integer getChangeOrgId(ChangeUsl changeUsl, LskCharge lskCharge) {
        Integer orgId;
        if (changeUsl.getOrg1Id() != null) {
            orgId = changeUsl.getOrg1Id(); // провести орг. заданной из перерасчета
        } else if (lskCharge.getChrgOrgId() != null) {
            orgId = lskCharge.getChrgOrgId(); // получить орг. из начисления
        } else {
            orgId = lskCharge.getNaborOrgId(); // получить орг. из наборов (старый вариант, когда начисление было без орг.)
        }
        return orgId;
    }

}
