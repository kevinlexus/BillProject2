package com.dic.app.service.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.service.*;
import com.dic.bill.dto.CommonResult;
import com.dic.bill.dto.LskChargeUsl;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileGen;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static com.dic.app.service.impl.enums.ProcessTypes.*;

@Slf4j
@Service
@RequiredArgsConstructor
public class ProcessAllMng {

    private final ConfigApp config;
    private final ThreadMng threadMng;
    private final GenChrgProcessMngImpl genChrgProcessMng;
    private final GenPenProcessMng genPenProcessMng;
    private final MigrateMng migrateMng;
    private final DistVolMng distVolMng;
    private final ApplicationContext ctx;


    /**
     * Отдельный поток для расчета длительных процессов
     *
     * @param reqConf конфиг запроса
     */
    @Async
    @Transactional(propagation = Propagation.REQUIRES_NEW, // todo todo todo todo зачем здесь REQUIRES_NEW ?? может для потока?
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
     * Выполнение процесса формирования начисления, задолженности, по помещению, по дому, по вводу - выполняется
     * например, из потоков распределения воды
     *
     * @param reqConf - конфиг запроса
     * @throws ErrorWhileGen - ошибка обработки
     */
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
     * Обработать очередь объектов
     *
     * @param reqConf конфиг запроса
     */
    private List<LskChargeUsl> selectInvokeProcess(RequestConfigDirect reqConf) throws ErrorWhileGen {
        List<LskChargeUsl> resultLskChargeUsl = new ArrayList<>();
        switch (reqConf.getTp()) { // начисление для распределения по вводу
            case CHARGE_0, DEBT_PEN_1, DIST_VOL_2, CHARGE_FOR_DIST_3, CHARGE_SINGLE_USL_4 -> {
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
                            if (Utl.in(reqConf.getTp(), CHARGE_0, DEBT_PEN_1, CHARGE_FOR_DIST_3, CHARGE_SINGLE_USL_4)) {
                                // Начисление и начисление для распределения объемов, расчет пени
                                if (reqConf.isSingleObjectCalc()) {
                                    log.info("****** {} фин.лиц.сч. klskId={} - начало    ******",
                                            reqConf.getTpName(), id);
                                }
                                if (Utl.in(reqConf.getTp(), DEBT_PEN_1)) {
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
                            } else if (reqConf.getTp() == DIST_VOL_2) {
                                // Распределение объемов
                                distVolMng.distVolByVvod(reqConf, id);
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
                    if (reqConf.getTp() == DIST_VOL_2) {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект vvodId=" + id);
                    } else {
                        throw new ErrorWhileGen("ОШИБКА! Произошла ошибка в потоке " + reqConf.getTpName()
                                + ", объект klskId=" + id);
                    }
                }
            }
            case MIGRATION_5 -> {
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
            }
            default -> throw new ErrorWhileGen("Некорректный параметр reqConf.tp=" + reqConf.getTp());
        }
        return resultLskChargeUsl;
    }


}
