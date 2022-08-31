package com.dic.app.mm.impl;

import com.dic.app.mm.ChangeMng;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.dto.*;
import com.dic.bill.enums.ChangeTps;
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
    public List<ResultChange> genChangesProc(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskChargeUsl>>> chargeByPeriod) {
        //log.info("klskId = {}", klskId);
        List<ResultChange> resultChanges = new ArrayList<>();

        // перебрать заданные периоды
        for (LocalDate dt = changesParam.getDtFrom(); dt.isBefore(changesParam.getDtTo().plusDays(1)); dt = dt.plusMonths(1)) {
            String period = Utl.getPeriodFromDate(dt);
            Map<String, List<LskChargeUsl>> chargesByUsl = chargeByPeriod.get(period);
            genChangesByProc(changesParam, resultChanges, dt, chargesByUsl);
        }
        return resultChanges;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public List<ResultChange> genChangesAbs(ChangesParam changesParam, Long klskId, Map<String, Map<String, List<LskNabor>>> naborsByPeriod) {
        final List<ResultChange> resultChanges = new ArrayList<>();
        // перебрать заданные периоды
        for (LocalDate dt = changesParam.getDtFrom(); dt.isBefore(changesParam.getDtTo().plusDays(1)); dt = dt.plusMonths(1)) {
            String period = Utl.getPeriodFromDate(dt);
            Map<String, List<LskNabor>> naborsByUsl = naborsByPeriod.get(period);
            genChangesByAbs(changesParam, resultChanges, dt, naborsByUsl);
        }
        return resultChanges;
    }

    private void genChangesByAbs(ChangesParam changesParam, List<ResultChange> resultChanges, LocalDate dt, Map<String, List<LskNabor>> naborsByUsl) {
        if (naborsByUsl != null) {
            for (ChangeUsl changeUsl : changesParam.getChangeUslList()) {
                List<LskNabor> nabors = naborsByUsl.get(changeUsl.getUslId());
                if (nabors != null) {
                    for (LskNabor nabor : nabors) {

                        Integer orgId = changeUsl.getOrgId() != null ? changeUsl.getOrgId() : nabor.getOrgId();
                        ResultChange resultChange = ResultChange.builder()
                                .lsk(nabor.getLsk())
                                .mg(nabor.getMg())
                                .uslId(changeUsl.getUslId())
                                .orgId(orgId)
                                .tp(ChangeTps.ABS)
                                .summa(changeUsl.getAbsSet()).build();
                        resultChanges.add(resultChange);
                    }
                }
            }
        }
    }

    private void genChangesByProc(ChangesParam changesParam, List<ResultChange> resultChanges, LocalDate dt, Map<String, List<LskChargeUsl>> chargesByUsl) {
        if (chargesByUsl != null) {
            // объем по услугам водоотведения
            List<LskChargeUsl> chargeWasteList = chargesByUsl.entrySet().stream()
                    .filter(t -> configApp.getWasteUslCodes().contains(t.getKey()))
                    .flatMap(t -> t.getValue().stream())
                    .collect(Collectors.toList());
            // объем по услугам водоотведения (ОДН)
            List<LskChargeUsl> chargeWasteOdnList = chargesByUsl.entrySet().stream()
                    .filter(t -> configApp.getWasteOdnUslCodes().contains(t.getKey()))
                    .flatMap(t -> t.getValue().stream())
                    .collect(Collectors.toList());
            for (ChangeUsl changeUsl : changesParam.getChangeUslList()) {
                if (changeUsl.getCntDays() != null && changeUsl.getCntDays() != 0) {
                    // получить процент из отношения кол-во дней перерасчета / кол-во дней месяца
                    changeUsl.setProc(BigDecimal.valueOf(changeUsl.getCntDays() * 100).divide(BigDecimal.valueOf(dt.lengthOfMonth()), 2, RoundingMode.HALF_UP));
                }

                // перебрать заданные в перерасчете услуги
                if (changeUsl.getProc() != null && changeUsl.getProc().compareTo(BigDecimal.ZERO) != 0) {
                    List<LskChargeUsl> charges = chargesByUsl.get(changeUsl.getUslId());
                    if (charges != null) {
                        for (LskChargeUsl lskCharge : charges) {
                            // перебрать начисление по услуге (может идти несколькими строками, разбито по орг)
                            if (lskCharge.getSumma().compareTo(BigDecimal.ZERO) != 0) {
                                BigDecimal sumChange = lskCharge.getSumma()
                                        .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                                        .multiply(changeUsl.getProc()).setScale(2, RoundingMode.HALF_UP);
                                BigDecimal volChange = lskCharge.getVol()
                                        .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                                        .multiply(changeUsl.getProc()).setScale(5, RoundingMode.HALF_UP);
                                Integer orgId = getChangeOrgId(changeUsl, lskCharge);
                                if (orgId == null) {
                                    log.error("Значение orgId должно быть либо задано пользователем, либо получено из начисления за период, или из наборов услуг");
                                    throw new RuntimeException("Пустое значение orgId по лиц.счету:" + lskCharge.getLsk());
                                }
                                ResultChange resultChange = ResultChange.builder()
                                        .lsk(lskCharge.getLsk())
                                        .mg(lskCharge.getMg())
                                        .uslId(changeUsl.getUslId())
                                        .orgId(orgId)
                                        .proc(changeUsl.getProc())
                                        .cntDays(changeUsl.getCntDays())
                                        .tp(ChangeTps.PROC)
                                        .summa(sumChange)
                                        .vol(volChange)
                                        .build();
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

    private List<ResultChange> getWastedChangesResult(List<LskChargeUsl> chargeWasteList, ChangeUsl changeUsl, LskChargeUsl lskCharge) {
        List<ResultChange> resultChanges = new ArrayList<>();
        for (LskChargeUsl lskChargeWaste : chargeWasteList) {
            if (lskChargeWaste.getVol() != null && lskChargeWaste.getVol().compareTo(BigDecimal.ZERO) != 0) {
                BigDecimal procWaste = lskCharge.getVol()
                        .divide(lskChargeWaste.getVol(), 10, RoundingMode.HALF_UP)
                        .multiply(changeUsl.getProc()).setScale(2, RoundingMode.HALF_UP);
                if (procWaste.compareTo(BigDecimal.ZERO) != 0) {
                    BigDecimal sumChange = lskChargeWaste.getSumma()
                            .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                            .multiply(procWaste).setScale(2, RoundingMode.HALF_UP);
                    BigDecimal volChange = lskChargeWaste.getVol()
                            .divide(BigDecimal.valueOf(100), 10, RoundingMode.HALF_UP)
                            .multiply(procWaste).setScale(5, RoundingMode.HALF_UP);
                    Integer orgId = getChangeOrgId(changeUsl, lskChargeWaste);
                    ResultChange resultChange = ResultChange.builder()
                            .lsk(lskChargeWaste.getLsk())
                            .mg(lskCharge.getMg())
                            .uslId(lskChargeWaste.getUslId())
                            .orgId(orgId)
                            .proc(procWaste)
                            .vol(volChange)
                            .cntDays(changeUsl.getCntDays())
                            .tp(ChangeTps.PROC)
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
    private Integer getChangeOrgId(ChangeUsl changeUsl, LskChargeUsl lskCharge) {
        Integer orgId;
        if (changeUsl.getOrgId() != null) {
            orgId = changeUsl.getOrgId(); // провести орг. заданной из перерасчета
        } else if (lskCharge.getOrgId() != null) {
            orgId = lskCharge.getOrgId(); // получить орг. из начисления
        } else {
            orgId = lskCharge.getNaborOrgId(); // получить орг. из наборов (старый вариант, когда начисление было без орг.)
        }
        return orgId;
    }

}
