package com.dic.app.mm.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.mm.*;
import com.dic.bill.dao.HouseDAO;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dto.*;
import com.dic.bill.enums.SelObjTypes;
import com.dic.bill.model.scott.*;
import com.ric.cmn.CommonConstants;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import com.ric.cmn.excp.WrongParam;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Lazy;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.*;
import java.util.concurrent.CompletableFuture;
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
    private final ThreadMng<Long> threadMng;
    private final GenChrgProcessMng genChrgProcessMng;
    private final GenPenProcessMng genPenProcessMng;
    private final MigrateMng migrateMng;
    private final ChangeMng changeMng;
    private final DistVolMng distVolMng;
    private final KartDAO kartDAO;
    private final HouseDAO houseDAO;
    private final ApplicationContext ctx;


    @Autowired
    public ProcessMngImpl(@Lazy DistVolMng distVolMng, ConfigApp config, ThreadMng<Long> threadMng,
                          GenChrgProcessMng genChrgProcessMng, GenPenProcessMng genPenProcessMng,
                          MigrateMng migrateMng, ChangeMng changeMng, KartDAO kartDAO, HouseDAO houseDAO, ApplicationContext ctx) {
        this.distVolMng = distVolMng;
        this.config = config;
        this.threadMng = threadMng;
        this.genChrgProcessMng = genChrgProcessMng;
        this.genPenProcessMng = genPenProcessMng;
        this.migrateMng = migrateMng;
        this.changeMng = changeMng;
        this.kartDAO = kartDAO;
        this.houseDAO = houseDAO;
        this.ctx = ctx;
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
                    ProcessMng processMng = ctx.getBean(ProcessMng.class); // возможно создание нового объекта и вызов processAll - чтобы был evict кэша?
                    processMng.processAll(reqConf);
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


    /**
     * Выполнение процесса формирования начисления, задолженности, по помещению, по дому, по вводу - выполняется
     * например, из потоков распределения воды
     *
     * @param reqConf - конфиг запроса
     * @throws ErrorWhileGen - ошибка обработки
     */
    @Override
    @CacheEvict(value = {"ReferenceMng.getUslOrgRedirect"}, allEntries = true)
    @Transactional(propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
    public List<LskChargeUsl> processAll(RequestConfigDirect reqConf) throws ErrorWhileGen {
        List<LskChargeUsl> resultChargeUsl;
        long startTime = System.currentTimeMillis();
        log.trace("НАЧАЛО процесса {} заданных объектов", reqConf.getTpName());
        // заблокировать, если нужно для долго длящегося процесса
        if (reqConf.isLockForLongLastingProcess()) {
            config.getLock().setLockProc(reqConf.getRqn(), reqConf.getStopMark());
        }
        try {
            // проверка остановки процесса
            boolean isCheckStop = false; // note решить что с этим делать!

            // ВЫЗОВ
            if (reqConf.isMultiThreads()) {
                // вызвать в новой транзакции, многопоточно
                resultChargeUsl = threadMng.invokeThreads(reqConf, reqConf.getRqn());
            } else {
                // не удалять комментарий, был случай выполнялось однопоточно на проде ред.09.03.21
                log.info("ВНИМАНИЕ! ВЫПОЛНЯЕТСЯ ОДНОПОТОЧНО!!!");
                log.info("ВНИМАНИЕ! ВЫПОЛНЯЕТСЯ ОДНОПОТОЧНО!!!");
                log.info("ВНИМАНИЕ! ВЫПОЛНЯЕТСЯ ОДНОПОТОЧНО!!!");
                // вызвать в той же транзакции, однопоточно, для Unit - тестов
                resultChargeUsl = selectInvokeProcess(reqConf);
            }

        } finally {
            // разблокировать долго длящийся процесс
            if (reqConf.isLockForLongLastingProcess()) {
                config.getLock().unlockProc(reqConf.getRqn(), reqConf.getStopMark());
            }
        }
        long endTime = System.currentTimeMillis();
        long totalTime;
        String tpTime;
        if (reqConf.isSingleObjectCalc()) {
            // один объект - время в мс.
            totalTime = (endTime - startTime);
            tpTime = "мс.";
        } else {
            // много объектов - время в мин.
            totalTime = (endTime - startTime) / 60000L;
            tpTime = "мин.";
        }
        log.trace("");
        log.trace("ОКОНЧАНИЕ процесса {} заданных объектов - Общее время выполнения = {} {}",
                reqConf.getTpName(), totalTime, tpTime);
        log.trace("");
        return resultChargeUsl;
    }


    /**
     * Отдельный поток для расчета длительных процессов
     *
     * @param reqConf - конфиг запроса
     */
    @Async
    @Override
    @Transactional(propagation = Propagation.REQUIRES_NEW,
            rollbackFor = Exception.class)
    public CompletableFuture<CommonResult> process(RequestConfigDirect reqConf) throws ErrorWhileGen {
        long startTime = System.currentTimeMillis();
        log.trace("НАЧАЛО потока {}", reqConf.getTpName());

        List<LskChargeUsl> lskChargeUsls = selectInvokeProcess(reqConf);
        CommonResult resultLskCharge = new CommonResult(lskChargeUsls);
        long endTime = System.currentTimeMillis();
        long totalTime = endTime - startTime;
        log.trace("ОКОНЧАНИЕ потока {}, время расчета={} мс", reqConf.getTpName(), totalTime);
        return CompletableFuture.completedFuture(resultLskCharge);
    }


    /**
     * Обработать очередь объектов
     *
     * @param reqConf - конфиг запроса
     * @return
     */
    private List<LskChargeUsl> selectInvokeProcess(RequestConfigDirect reqConf) throws ErrorWhileGen {
        List<LskChargeUsl> resultLskChargeUsl = new ArrayList<>();
        switch (reqConf.getTp()) {
            case 0:
            case 1:
            case 2:
            case 3: // начисление для распределения по вводу
            case 4: {
                // перебрать все объекты для расчета
                Long id = null;
                try {
                    long i = 0, i2 = 0;
                    while (true) {
                        id = reqConf.getNextItem();
                        if (id != null) {
                            if (reqConf.isLockForLongLastingProcess() && config.getLock().isStopped(reqConf.getStopMark())) {
                                log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                                break;
                            }
                            if (Utl.in(reqConf.getTp(), 0, 1, 3, 4)) {
                                // Начисление и начисление для распределения объемов, расчет пени
                                if (reqConf.isSingleObjectCalc()) {
                                    log.info("****** {} фин.лиц.сч. klskId={} - начало    ******",
                                            reqConf.getTpName(), id);
                                }
                                if (Utl.in(reqConf.getTp(), 1)) {
                                    // расчет пени
                                    genPenProcessMng.genDebitPen(reqConf, true, id);
                                } else {
                                    // расчет начисления и начисления для распределения объемов
                                    resultLskChargeUsl.addAll(genChrgProcessMng.genChrg(reqConf, id));
                                }
                                if (reqConf.isSingleObjectCalc()) {
                                    log.info("****** {} фин.лиц.сч. klskId={} - окончание   ******",
                                            reqConf.getTpName(), id);
                                } else if (i == 500L) {
                                    i = 0;
                                    log.info("****** Поток {}, {}, обработано {} объектов  ******",
                                            Thread.currentThread().getName(), reqConf.getTpName(), i2);
                                }
                                i++;
                                i2++;
                            } else if (reqConf.getTp() == 2) {
                                // Распределение объемов
                                distVolMng.distVolByVvodTrans(reqConf, id);
                            }
                        } else {
                            // перебраны все id, выход
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(Utl.getStackTraceString(e));
                    // остановить другие потоки
                    if (reqConf.isLockForLongLastingProcess()) {
                        config.getLock().unlockProc(reqConf.getRqn(), reqConf.getStopMark());
                    }
                    if (reqConf.getTp() == 2) {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект vvodId=" + id);
                    } else {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект klskId=" + id);
                    }
                }
                break;
            }
            case 5: {
                // миграция долгов
                // перебрать все объекты для расчета
                String id = null;
                try {
                    long i = 0;
                    while (true) {
                        id = reqConf.getNextStrItem();
                        if (id != null) {
                            if (reqConf.isLockForLongLastingProcess() && config.getLock().isStopped(reqConf.getStopMark())) {
                                log.info("Процесс {} был ПРИНУДИТЕЛЬНО остановлен", reqConf.getTpName());
                                break;
                            }
                            migrateMng.migrateDeb(id, Integer.valueOf(config.getPeriodBack()),
                                    Integer.valueOf(config.getPeriod()), reqConf.getDebugLvl());
                            log.info("****** Поток {}, {}, обработано {} объектов ******",
                                    Thread.currentThread().getName(), reqConf.getTpName(), i);
                            i++;
                        } else {
                            // перебраны все id, выход
                            break;
                        }
                    }
                } catch (Exception e) {
                    log.error(Utl.getStackTraceString(e));
                    throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                            + ", объект lsk=" + id);
                }
                break;
            }
            default:
                throw new ErrorWhileGen("Некорректный параметр reqConf.tp=" + reqConf.getTp());
        }
        return resultLskChargeUsl;
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public int processChanges(ChangesParam changesParam) throws ExecutionException, InterruptedException, ErrorWhileGen, WrongParam {
        if (Integer.parseInt(changesParam.getPeriodTo()) > Integer.parseInt(config.getPeriod())) {
            throw new WrongParam("Замыкающий период перерасчета " + changesParam.getPeriodTo() + " не может быть позже текущего" + changesParam.getPeriodTo());
        }

        if (!CollectionUtils.isEmpty(changesParam.getSelObjList())) {
            // Получить список объектов
            List<String> kulNds;
            Set<Long> klskIds = new HashSet<>();
            if (1 == 1 || changesParam.getSelObjList().stream().anyMatch(t -> t.getTp().equals(SelObjTypes.ALL))) {
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
                List<String> lsks = changesParam.getSelObjList().stream()
                        .filter(t -> t.getTp().equals(SelObjTypes.LSK))
                        .map(Selobj::getLsk).collect(Collectors.toList());
                if (lsks.size() > 0) {
                    klskIds.addAll(kartDAO.findKlskIdByLsk(lsks));
                }
            }

            // Получить текущее начисление по объектам
            List<LskChargeUsl> currentCharges = new ArrayList<>();
            if (Utl.between(Integer.parseInt(config.getPeriod()),
                    Integer.parseInt(changesParam.getPeriodFrom()), Integer.parseInt(changesParam.getPeriodTo()))) {
                currentCharges = getCurrentCharges(kulNds, klskIds);
            }
            currentCharges.forEach(t -> log.info("Текущее начисление klskId={}, lsk={}, uslId={}, orgId={}, area={}, summa={}, price={}",
                    t.getKLskId(), t.getLsk(), t.getUslId(), t.getOrgId(), t.getArea(), t.getSumma(), t.getPrice()));

            // Получить архивное начисление по объектам
            List<LskCharge> archCharges = getArchCharges(changesParam, kulNds, klskIds);

            // Map устроен: klskId, (mg, (uslId, List<LskCharges>))
            Map<Long, Map<String, Map<String, List<LskCharge>>>> chargesByKlskId;
            chargesByKlskId = archCharges.stream().collect(groupingBy(LskCharge::getKlskId, groupingBy(LskCharge::getMg, groupingBy(LskCharge::getUslId))));

            // Выполнение перерасчета
            List<ResultChange> resultChanges = chargesByKlskId.entrySet().parallelStream()
                    .flatMap(t -> changeMng.genChanges(changesParam, t.getKey(), t.getValue()).stream())
                    .collect(Collectors.toList());
            log.info("resultChanges size={}", resultChanges.size());

            for (ResultChange resultChange : resultChanges) {
                log.info("Перерасчет по lsk={}, period={}, usl={}, org={}, proc={}, составил summa={}",
                        resultChange.getLsk(), resultChange.getMg(), resultChange.getUslId(),
                        resultChange.getOrg1Id(), resultChange.getProc1(), resultChange.getSumma());
            }
        } else {
            log.warn("Не найдены объекты, для перерасчета!");
        }

        return 0;
    }

    /**
     * Получить архивное начисление по объектам
     *
     * @param changesParam параметры перерасчета
     * @param kulNds       список домов
     * @param klskIds      список фин.лиц.сч.
     */

    private List<LskCharge> getArchCharges(ChangesParam changesParam, List<String> kulNds, Set<Long> klskIds) {
        String archPeriodTo;
        if (Integer.parseInt(changesParam.getPeriodTo()) == Integer.parseInt(config.getPeriod())) {
            archPeriodTo = config.getPeriodBack();
        } else {
            archPeriodTo = changesParam.getPeriodTo();
        }

        List<String> uslListForQuery = changesParam.getChangeUslList().stream().map(ChangeUsl::getUslId).collect(Collectors.toList());
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
            log.info("Перерасчет по домам:");
            archCharges = kartDAO.getArchChargesByKulNd(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), archPeriodTo,
                    kulNds, uslListForQuery
            );
        }
        if (klskIds.size() > 0) {
            log.info("Перерасчет по фин.лиц.сч:");
            archCharges = kartDAO.getArchChargesByKlskIds(changesParam.getProcessStatus(), changesParam.getProcessMeter(),
                    changesParam.getProcessAccount(), changesParam.getProcessEmpty(),
                    changesParam.getProcessKran(), changesParam.getProcessLskTp(),
                    changesParam.getPeriodFrom(), archPeriodTo,
                    klskIds, uslListForQuery
            );
        }
        archCharges.forEach(t -> log.info("Начисление по lsk={}, mg={}, usl={}, chrg.org={}, nabor.org={}, составило summa={}",
                t.getLsk(), t.getMg(), t.getUslId(), t.getChrgOrgId(), t.getNaborOrgId(), t.getSumma()));
        return archCharges;
    }

    /**
     * Получить текущее начисление по объектам
     *
     * @param kulNds  список домов
     * @param klskIds список фин.лиц.сч.
     */
    private List<LskChargeUsl> getCurrentCharges(List<String> kulNds, Set<Long> klskIds) throws ErrorWhileGen {
        Set<Long> klskIdsCharged = new HashSet<>(); // для исключения дублей в начислении
        Set<Long> klskIdsForCharge = new HashSet<>(klskIds);
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
            lskChargeUsl = processAll(reqConf);
            lskChargeUsl.forEach(t -> klskIdsCharged.add(t.getKLskId()));
        }
        log.info("Кол-во записей начисления по домам {}", lskChargeUsl.size());

        klskIdsForCharge.removeIf(klskIdsCharged::contains);
        if (klskIdsForCharge.size() > 0) {
            // по списку фин.лиц.сч.
            genChargeByKlskIds(lskChargeUsl, klskIdsForCharge);
        }
        log.info("Кол-во записей начисления по фин.лиц.сч. {}", lskChargeUsl.size());
        return lskChargeUsl;
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
        List<LskChargeUsl> charges = processAll(reqConf);
        log.info("Кол-во записей начисления {}", charges.size());
        lskChargeUsl.addAll(charges);
    }


}