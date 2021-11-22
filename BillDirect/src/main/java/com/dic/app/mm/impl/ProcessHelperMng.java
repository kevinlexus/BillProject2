package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.ChangesParam;
import com.dic.bill.dto.LskCharge;
import com.dic.bill.dto.LskChargeUsl;
import com.dic.bill.dto.LskNabor;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

@Service
@Slf4j
@RequiredArgsConstructor
public class ProcessHelperMng {

    private final ConfigApp config;
    private final KartDAO kartDAO;
    private final ProcessAllMng processAllMng;

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public Map<Long, Map<String, Map<String, List<LskChargeUsl>>>> getChargesByKlskId(ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) throws ErrorWhileGen {
        // Получить текущее начисление по объектам
        final List<LskChargeUsl> currentCharges = new ArrayList<>();
        if (Utl.between(Integer.parseInt(config.getPeriod()),
                Integer.parseInt(changesParam.getPeriodFrom()), Integer.parseInt(changesParam.getPeriodTo()))) {
            getCurrentCharges(currentCharges, kulNds, klskIds);
        }

        // Получить архивное начисление по объектам
        if (Integer.parseInt(changesParam.getPeriodFrom()) < Integer.parseInt(config.getPeriod())) {
            getArchCharges(currentCharges, changesParam, kulNds, klskIds);
        }

        // Возвращаемый Map устроен: klskId, (mg, (uslId, List<LskChargeUsl>))
        return currentCharges.stream().collect(groupingBy(LskChargeUsl::getKlskId, groupingBy(LskChargeUsl::getMg,
                groupingBy(LskChargeUsl::getUslId))));
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void getCurrentNabors(List<LskNabor> currentNabors, ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) {
        List<String> uslListForQuery = changesParam.getUslListForQuery();
        List<LskNabor> nabors = new ArrayList<>();
        Set<Long> klskIdsProcessed = new HashSet<>(); // для исключения дублей в наборах услуг
        Set<Long> klskIdsForProcess = new HashSet<>(klskIds);

        if (kulNds.size() > 0) {
            nabors = kartDAO.getNaborsByKulNd(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    kulNds, uslListForQuery);
            nabors.forEach(t -> klskIdsProcessed.add(t.getKlskId()));
        }

        klskIdsForProcess.removeIf(klskIdsProcessed::contains);
        if (klskIds.size() > 0) {
            List<LskNabor> klskIdNabors = kartDAO.getNaborsByKlskIds(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    klskIdsForProcess, uslListForQuery);
            nabors.addAll(klskIdNabors);
        }
        currentNabors.addAll(nabors);
    }


    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void getArchNabors(List<LskNabor> currentNabors, ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) {
        List<String> uslListForQuery = changesParam.getUslListForQuery();
        List<LskNabor> nabors = new ArrayList<>();
        Set<Long> klskIdsProcessed = new HashSet<>(); // для исключения дублей в наборах услуг
        Set<Long> klskIdsForProcess = new HashSet<>(klskIds);

        if (kulNds.size() > 0) {
            nabors = kartDAO.getArchNaborsByKulNd(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), changesParam.getArchPeriodTo(),
                    kulNds, uslListForQuery);
            nabors.forEach(t -> klskIdsProcessed.add(t.getKlskId()));
        }

        klskIdsForProcess.removeIf(klskIdsProcessed::contains);
        if (klskIds.size() > 0) {
            List<LskNabor> klskIdNabors = kartDAO.getArchNaborsByKlskIds(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), changesParam.getArchPeriodTo(),
                    klskIdsForProcess, uslListForQuery);
            nabors.addAll(klskIdNabors);
        }
        currentNabors.addAll(nabors);
    }


    /**
     * Получить архивное начисление по объектам
     *
     * @param currentCharges
     * @param changesParam   параметры перерасчета
     * @param kulNds         список домов
     * @param klskIds        список фин.лиц.сч.
     */

    private void getArchCharges(List<LskChargeUsl> currentCharges, ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) {
        List<String> uslListForQuery = changesParam.getUslListForQuery();
        Set<Long> klskIdsProcessed = new HashSet<>(); // для исключения дублей в наборах услуг
        Set<Long> klskIdsForProcess = new HashSet<>(klskIds);
        if (changesParam.getIsAddUslWaste()) {
            // добавить услуги по водоотведению
            if (uslListForQuery.stream().anyMatch(t -> config.getWaterUslCodes().contains(t))) {
                uslListForQuery.addAll(config.getWasteUslCodes());
            } else if (uslListForQuery.stream().anyMatch(t -> config.getWaterOdnUslCodes().contains(t))) {
                uslListForQuery.addAll(config.getWasteOdnUslCodes());
            }
        }

        List<LskCharge> archCharges = new ArrayList<>();
        if (kulNds.size() > 0) {
            archCharges = kartDAO.getArchChargesByKulNd(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), changesParam.getArchPeriodTo(),
                    kulNds, uslListForQuery
            );
            archCharges.forEach(t -> klskIdsProcessed.add(t.getKlskId()));
        }

        klskIdsForProcess.removeIf(klskIdsProcessed::contains);
        if (klskIds.size() > 0) {
            List<LskCharge> klskIdsCharges = kartDAO.getArchChargesByKlskIds(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), changesParam.getArchPeriodTo(),
                    klskIdsForProcess, uslListForQuery
            );
            archCharges.addAll(klskIdsCharges);
        }
        currentCharges.addAll(archCharges.stream().map(t -> LskChargeUsl.builder()
                .klskId(t.getKlskId()).lsk(t.getLsk()).mg(t.getMg()).uslId(t.getUslId())
                .orgId(t.getOrgId()).naborOrgId(t.getNaborOrgId())
                .summa(t.getSumma()).vol(t.getVol()).build()).collect(Collectors.toList()));
    }

    /**
     * Получить текущее начисление по объектам
     *
     * @param currentCharges
     * @param kulNds         список домов
     * @param klskIds        список фин.лиц.сч.
     */
    private void getCurrentCharges(List<LskChargeUsl> currentCharges, List<String> kulNds, Set<Long> klskIds) throws ErrorWhileGen {
        Set<Long> klskIdsProcessed = new HashSet<>(); // для исключения дублей в начислении
        Set<Long> klskIdsForProcess = new HashSet<>(klskIds);
        List<LskChargeUsl> lskChargeUsl = new ArrayList<>();
        if (kulNds.size() > 0) {
            // по списку домов
            RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                    .withTp(0)
                    .withGenDt(config.getCurDt2())
                    .withCurDt1(config.getCurDt1())
                    .withCurDt2(config.getCurDt2())
                    .withKulNds(kulNds)
                    .withDebugLvl(1)
                    .withRqn(config.incNextReqNum())
                    .withIsMultiThreads(true)
                    .withStopMark("processMng.process")
                    .withsaveResult(false)
                    .build();
            reqConf.prepareId();
            lskChargeUsl = processAllMng.processAll(reqConf);
            lskChargeUsl.forEach(t -> klskIdsProcessed.add(t.getKlskId()));
        }

        klskIdsForProcess.removeIf(klskIdsProcessed::contains);
        if (klskIdsForProcess.size() > 0) {
            // по списку фин.лиц.сч.
            genChargeByKlskIds(lskChargeUsl, klskIdsForProcess);
        }
        currentCharges.addAll(lskChargeUsl);
    }

    private void genChargeByKlskIds(List<LskChargeUsl> lskChargeUsl, Set<Long> klskIds) throws ErrorWhileGen {

        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(0)
                .withGenDt(config.getCurDt2())
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withKlskIds(new ArrayList<>(klskIds))
                .withDebugLvl(1)
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(true)
                .withStopMark("processMng.process")
                .withsaveResult(false)
                .build();
        reqConf.prepareId();
        List<LskChargeUsl> charges = processAllMng.processAll(reqConf);
        log.info("Кол-во записей начисления {}", charges.size());
        lskChargeUsl.addAll(charges);
    }

}
