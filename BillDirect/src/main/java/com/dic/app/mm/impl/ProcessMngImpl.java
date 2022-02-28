package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.ChangeMng;
import com.dic.app.mm.ConfigApp;
import com.dic.app.mm.ProcessMng;
import com.dic.bill.dao.ChangeDocDAO;
import com.dic.bill.dao.HouseDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.dto.*;
import com.dic.bill.enums.SelObjTypes;
import com.dic.bill.model.scott.*;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import com.ric.cmn.excp.WrongParamPeriod;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.groupingBy;

/**
 * Сервис выполнения процессов формирования
 *
 * @author lev
 * @version 1.12
 */
@Slf4j
@Service
@Scope("prototype")
public class ProcessMngImpl implements ProcessMng, CommonConstants {

    private final ConfigApp config;
    private final ChangeMng changeMng;
    private final KartDAO kartDAO;
    private final TuserDAO tuserDAO;
    private final HouseDAO houseDAO;
    private final ProcessAllMng processAllMng;
    private final ProcessHelperMng processHelperMng;
    private final ChangeDocDAO changeDocDAO;


    @Autowired
    public ProcessMngImpl(ConfigApp config,
                          ChangeMng changeMng, KartDAO kartDAO, TuserDAO tuserDAO, HouseDAO houseDAO, ProcessAllMng processAllMng, ProcessHelperMng processHelperMng, ChangeDocDAO changeDocDAO) {
        this.config = config;
        this.changeMng = changeMng;
        this.kartDAO = kartDAO;
        this.tuserDAO = tuserDAO;
        this.houseDAO = houseDAO;
        this.processAllMng = processAllMng;
        this.processHelperMng = processHelperMng;
        this.changeDocDAO = changeDocDAO;
    }

    /**
     * Обработка запроса  из WebController
     *
     * @param tp       тип выполнения 0-начисление, 1-задолженность и пеня, 2 - распределение объемов по вводу,
     *                 4 - начисление по одной услуге, для автоначисления
     * @param debugLvl уровень отладки
     * @param genDt    дата формирования
     * @param house    дом
     * @param vvod     ввод
     * @param ko       объект Ко
     * @param uk       УК
     * @param usl      услуга
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            isolation = Isolation.READ_COMMITTED, // читать только закомиченные данные, не ставить другое, не даст запустить поток!
            rollbackFor = Exception.class)
    public String processWebRequest(int tp, int debugLvl, Date genDt,
                                    House house, Vvod vvod, Ko ko, Org uk, Usl usl) {
        String retStatus;
        // построить запрос
        RequestConfigDirect reqConf = RequestConfigDirect.RequestConfigDirectBuilder.aRequestConfigDirect()
                .withTp(tp)
                .withGenDt(genDt)
                .withUk(uk)
                .withHouse(house)
                .withVvod(vvod)
                .withKo(ko)
                .withUsl(usl)
                .withCurDt1(config.getCurDt1())
                .withCurDt2(config.getCurDt2())
                .withDebugLvl(debugLvl)
                .withRqn(config.incNextReqNum())
                .withIsMultiThreads(true)
                .withStopMark("processMng.process")
                .build();
        reqConf.prepareChrgCountAmount(); // fixme сделал, потому что в Полыс. валилось в автоначислении: java.lang.NullPointerException: Cannot invoke "com.dic.bill.dto.ChrgCountAmount.setResultVol(java.math.BigDecimal) pp.mm.impl.GenChrgProcessMngImpl.genChrg(GenChrgProcessMngImpl.java:181)
        reqConf.prepareId();
        //StopWatch sw = new StopWatch();
        //sw.start("TIMING: " + reqConf.getTpName());

        // проверить переданные параметры
        retStatus = reqConf.checkArguments();
        if (retStatus == null) {
            try {
                if (Utl.in(reqConf.getTp(), 0, 1, 2, 4)) {
                    // расчет начисления, распределение объемов, расчет пени
                    if (reqConf.getLstItems().size() > 1)
                        log.info("Будет обработано {} объектов", reqConf.getLstItems().size());
                    processAllMng.processAll(reqConf);
                }
                if (Utl.in(reqConf.getTp(), 4)) {
                    // по операции - начисление по одной услуге, для автоначисления
                    // вернуть начисленный объем
                    retStatus = "OK:" + reqConf.getChrgCountAmount().getResultVol().toString();
                } else {
                    retStatus = "OK";
                }
            } catch (Exception e) {
                retStatus = "ERROR! Ошибка при выполнении расчета!";
                log.error(Utl.getStackTraceString(e));
            }
        }
        //sw.stop();
        //System.out.println(sw.prettyPrint());
        return retStatus;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public int processChanges(ChangesParam changesParam) throws ExecutionException, InterruptedException, ErrorWhileGen, JsonProcessingException, WrongParamPeriod, WrongParam {
        if (Integer.parseInt(changesParam.getPeriodTo()) > Integer.parseInt(config.getPeriod())) {
            throw new WrongParamPeriod("Замыкающий период перерасчета " + Utl.getPeriodYear(changesParam.getPeriodTo()) + "-" + Utl.getPeriodMonth(changesParam.getPeriodTo())
                    + " не может быть позже текущего" + Utl.getPeriodYear(config.getPeriod()) + "-" + Utl.getPeriodMonth(config.getPeriod()));
        }
        if (Integer.parseInt(changesParam.getPeriodFrom()) > Integer.parseInt(changesParam.getPeriodTo())) {
            throw new WrongParamPeriod("Некорректный период перерасчета");
        }
        int changeDocId = 0;

        List<ResultChange> resultChanges = new ArrayList<>();
        if (!CollectionUtils.isEmpty(changesParam.getSelObjList())) {
            // Получить список объектов
            List<String> kulNds;
            Set<Long> klskIds = new HashSet<>();
            boolean isAllObj = changesParam.getSelObjList().stream().anyMatch(t -> t.getTp().equals(SelObjTypes.ALL));
            Optional<Selobj> lskRange = changesParam.getSelObjList().stream()
                    .filter(t -> t.getTp().equals(SelObjTypes.LSK))
                    .findAny();
            lskRange.ifPresent(changesParam::setLskRange);
            if (isAllObj) {
                // весь фонд. Выбрать все дома
                kulNds = houseDAO.getAllKulNds();
            } else {
                // выборочно
                kulNds = changesParam.getSelObjList().stream()
                        .filter(t -> t.getTp().equals(SelObjTypes.HOUSE))
                        .map(t -> t.getKul() + t.getNd()).collect(Collectors.toList());
                klskIds = changesParam.getSelObjList().stream()
                        .filter(t -> t.getTp().equals(SelObjTypes.FIN_ACCOUNT))
                        .map(Selobj::getKlskId).collect(Collectors.toSet());
                if (lskRange.isPresent()) {
                    klskIds.addAll(kartDAO.findKlskIdByLsk(lskRange.get().getLskFrom(), lskRange.get().getLskTo()));
                }
            }

            boolean isExistProc = changesParam.getChangeUslList().stream()
                    .anyMatch(t -> t.getProc() != null && t.getProc().compareTo(BigDecimal.ZERO) != 0 || t.getCntDays() != null && t.getCntDays() != 0);
            boolean isExistAbs = changesParam.getChangeUslList().stream()
                    .anyMatch(t -> t.getAbsSet() != null && t.getAbsSet().compareTo(BigDecimal.ZERO) != 0);
            LocalDate dtFrom = LocalDate.of(Integer.parseInt(changesParam.getPeriodFrom().substring(0, 4)),
                    Integer.parseInt(changesParam.getPeriodFrom().substring(4)), 1);
            LocalDate dtTo = LocalDate.of(Integer.parseInt(changesParam.getPeriodTo().substring(0, 4)),
                    Integer.parseInt(changesParam.getPeriodTo().substring(4)), 1);
            String archPeriodTo;
            if (Integer.parseInt(changesParam.getPeriodTo()) == Integer.parseInt(config.getPeriod())) {
                archPeriodTo = config.getPeriodBack();
            } else {
                archPeriodTo = changesParam.getPeriodTo();
            }
            changesParam.setArchPeriodTo(archPeriodTo);
            changesParam.setDtFrom(dtFrom);
            changesParam.setDtTo(dtTo);
            changesParam.setUslListForQuery(changesParam.getChangeUslList().stream().map(ChangeUsl::getUslId).collect(Collectors.toList()));

            // ВЫПОЛНИТЬ ПЕРЕРАСЧЕТ
            if (isExistProc) {
                Map<Long, Map<String, Map<String, List<LskChargeUsl>>>> chargesByKlskId = processHelperMng.getChargesByKlskId(changesParam, kulNds, klskIds);
                resultChanges = chargesByKlskId.entrySet().parallelStream()
                        .flatMap(t -> changeMng.genChangesProc(changesParam, t.getKey(), t.getValue()).stream())
                        .filter(t -> lskRange.isEmpty() ||
                                Utl.between(t.getLsk(), lskRange.get().getLskFrom(), lskRange.get().getLskTo()))
                        .collect(Collectors.toList());
            }
            if (isExistAbs) {
                Map<Long, Map<String, Map<String, List<LskNabor>>>> naborByKlskId = getNaborsByKlskId(changesParam, kulNds, klskIds);
                resultChanges.addAll(naborByKlskId.entrySet().parallelStream()
                        .flatMap(t -> changeMng.genChangesAbs(changesParam, t.getKey(), t.getValue()).stream())
                        .collect(Collectors.toList()));
            }

            log.info("resultChanges size={}", resultChanges.size());

            // сохранить перерасчет
            if (resultChanges.size() > 0) {
                Tuser user = tuserDAO.findByCd(changesParam.getUser().toUpperCase());
                ChangeDoc changeDoc = new ChangeDoc();
                changeDoc.setDt(Utl.getDateTruncated(changesParam.getDt()));
                changeDoc.setMgchange(changesParam.getPeriodFrom()); // todo убрать период - на фиг не нужен
                changeDoc.setUserId(user.getId());
                changeDoc.setText(changesParam.getComment());
                ObjectMapper objectMapper = new ObjectMapper();
                changeDoc.setParamJson(objectMapper.writeValueAsString(changesParam));
                changeDocDAO.save(changeDoc);
                changeDocId = changeDoc.getId();

                for (ResultChange resultChange : resultChanges) {
/*
                    if (resultChange.getLsk().equals("00000007")) {
                        log.info("Перерасчет по lsk={}, period={}, usl={}, org={}, proc={}, составил summa={}",
                                resultChange.getLsk(), resultChange.getMg(), resultChange.getUslId(),
                                resultChange.getOrgId(), resultChange.getProc(), resultChange.getSumma());
                    }
*/

                    Change change = new Change();
                    change.setChangeDoc(changeDoc);
                    change.setLsk(resultChange.getLsk());
                    change.setUslId(resultChange.getUslId());
                    change.setOrgId(resultChange.getOrgId());
                    change.setOrgId(resultChange.getOrgId());
                    change.setUserId(user.getId());
                    change.setMg2(changesParam.getPeriodProcess() != null ? changesParam.getPeriodProcess() : resultChange.getMg());
                    change.setMgchange(resultChange.getMg());
                    change.setSumma(resultChange.getSumma());
                    change.setDt(Utl.getDateTruncated(changesParam.getDt()));
                    change.setTp(resultChange.getTp());
                    change.setProc(resultChange.getProc());
                    change.setCntDays(resultChange.getCntDays());
                    changeDoc.getChange().add(change);

                }
            }
        } else {
            throw new WrongParam("Не найдены параметры для перерасчета");
        }

        return changeDocId;
    }

    private Map<Long, Map<String, Map<String, List<LskNabor>>>> getNaborsByKlskId(ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) throws ErrorWhileGen {
        final List<LskNabor> currentNabors = new ArrayList<>();
        // Получить текущие наборы услуг по объектам
        if (Utl.between(Integer.parseInt(config.getPeriod()),
                Integer.parseInt(changesParam.getPeriodFrom()), Integer.parseInt(changesParam.getPeriodTo()))) {
            processHelperMng.getCurrentNabors(currentNabors, changesParam, kulNds, klskIds);
        }

        // Получить архивные наборы услуг по объектам
        if (Integer.parseInt(changesParam.getPeriodFrom()) < Integer.parseInt(config.getPeriod())) {
            processHelperMng.getArchNabors(currentNabors, changesParam, kulNds, klskIds);
        }

        log.info("Начало проверки на дубли nabor");
        boolean existsDuplicates = currentNabors.stream().anyMatch(t -> currentNabors.stream()
                .filter(d -> changesParam.getLskRange() != null ||
                        Utl.between(t.getLsk(), changesParam.getLskRange().getLskFrom(), changesParam.getLskRange().getLskTo()))
                .anyMatch(d -> !t.equals(d) && d.getLsk().equals(t.getLsk()) && d.getMg().equals(t.getMg()) && d.getUslId().equals(t.getUslId())
                ));
        if (existsDuplicates) {
            throw new ErrorWhileGen("Обнаружены дубли в полученном списке услуг");
        }
        log.info("Окончание проверки на дубли nabor");

        // Возвращаемый Map устроен: klskId, (mg, (uslId, List<LskNabor>))
        return currentNabors.stream().collect(groupingBy(LskNabor::getKlskId, groupingBy(LskNabor::getMg,
                groupingBy(LskNabor::getUslId))));

    }


}