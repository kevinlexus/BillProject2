package com.dic.app.mm.impl;

import com.dic.app.mm.ComprTbl;
import com.dic.app.mm.MntBase;
import com.dic.bill.dao.*;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Param;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.WrongTableException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;

/**
 * Cервис обслуживания базы данных
 *
 * @author lev
 */
@Slf4j
@Service
public class MntBaseImpl implements MntBase {

    private final AnaborDAO anaborDao;
    private final AchargeDAO achargeDao;
    private final AchargePrepDAO achargePrepDao;
    private final AkartPrDAO akartPrDAO;
    private final ParamDAO paramDao;
    private final ChargePayDAO chargePayDAO;
    private final ComprTbl comprTbl;

    // текущий период
    private Integer curPeriod;
    // период -2 от текущего
    private Integer backPeriod;
    // анализировать все периоды?
    private boolean isAllPeriods;

    public MntBaseImpl(AnaborDAO anaborDao, AchargeDAO achargeDao, AchargePrepDAO achargePrepDao,
                       AkartPrDAO akartPrDAO, ParamDAO paramDao, ChargePayDAO chargePayDAO, ComprTbl comprTbl) {
        this.anaborDao = anaborDao;
        this.achargeDao = achargeDao;
        this.achargePrepDao = achargePrepDao;
        this.akartPrDAO = akartPrDAO;
        this.paramDao = paramDao;
        this.chargePayDAO = chargePayDAO;
        this.comprTbl = comprTbl;
    }

    /**
     * Сжать таблицу
     *
     * @param table    - класс таблицы
     * @param firstLsk - начать с лицевого
     * @param oneLsk   - только этот лицевой
     */
    private void comprTable(String table, String firstLsk, String oneLsk) throws ExecutionException, InterruptedException, WrongTableException {
        int startTime;
        int endTime;
        int totalTime;
        log.info("Compress table:{}", table);
        startTime = (int) System.currentTimeMillis();

        List<String> lstLsk;
        // Получить список лс
        if (oneLsk != null) {
            lstLsk = new ArrayList<>();
            lstLsk.add(oneLsk);
        } else {
            switch (table) {
                case "anabor" -> lstLsk = anaborDao.getAfterLsk(firstLsk).stream().map(Kart::getLsk)
                        .collect(Collectors.toList());
                case "acharge" -> lstLsk = achargeDao.getAfterLsk(firstLsk).stream().map(Kart::getLsk)
                        .collect(Collectors.toList());
                case "achargeprep" -> lstLsk = achargePrepDao.getAfterLsk(firstLsk).stream().map(Kart::getLsk)
                        .collect(Collectors.toList());
                case "akartpr" -> lstLsk = akartPrDAO.getAfterLsk(firstLsk).stream().map(Kart::getLsk)
                        .collect(Collectors.toList());
                case "chargepay" -> lstLsk = chargePayDAO.getAfterLsk(firstLsk).stream().map(Kart::getLsk)
                        .collect(Collectors.toList());
                default -> {
                    log.error("Ошибка! Некорректный класс таблицы:{}", table);
                    throw new WrongTableException("Некорректный класс таблицы!");
                }
            }

        }
        ForkJoinPool forkJoinPool = new ForkJoinPool(16);
        forkJoinPool.submit(() -> lstLsk.parallelStream().forEach(lsk ->
                comprTbl.comprTableByLsk(table, lsk, backPeriod, curPeriod, isAllPeriods))).get();

/*
		Queue<String> qu = new LinkedList<>(lstLsk);
			cnt = 1;
			avgLst = new TreeList<>();
		// выйти, если нет лс для обработки
		while (qu.size() != 0) {
			// Получить очередную пачку лицевых
			List<String> batch = new LinkedList<>();

			if (setBestCntThread) {
				cntThread = bestCntThread;
			}
			for (int b = 1; b <= cntThread; b++) {
				String addLsk = qu.poll();
				if (addLsk == null) {
					break;
				} else {
					batch.add(addLsk);
				}
			}

			long startTime2;
			long endTime2;
			startTime2 = System.currentTimeMillis();
			List<Future<Result>> frl = new ArrayList<>();

			// Начать потоки
			for (String lsk : batch) {
				ComprTbl comprTbl = ctx.getBean(ComprTbl.class);
				//Future<Result> fut = comprTbl.comprTableByLsk(table, lsk, backPeriod, curPeriod, isAllPeriods);
				frl.add(fut);
				if (cnt == 1000) {
					log.info("Последний лс на обработке={}", lsk);
					cnt = 1;
				} else {
					cnt++;
				}
			}

			// проверить окончание всех потоков
			int flag2 = 0;
			while (flag2 == 0) {
				//log.info("========================================== Waiting for threads");
				flag2 = 1;
				for (Future<Result> fut : frl) {
					if (!fut.isDone()) {
						flag2 = 0;
					} else {
						try {
							if (fut.get().getErr() != 0) {
								throw new Exception("Ошибка в потоке err=" + fut.get().getErr());
							}
						} catch (InterruptedException | ExecutionException e) {
							log.error(Utl.getStackTraceString(e));
						}
					}
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}

			}

			endTime2 = System.currentTimeMillis();
			batchTime = (int) ((endTime2 - startTime2) / cntThread);
			//log.info("Выполнение одного лс используя {} потоков заняло {} мс", cntThread, batchTime);

			// проверить время выполнения пачки
			if (!setBestCntThread) {
				avgLst.add(batchTime);
				// пока не установлено лучшее кол-во потоков
				if (batchCnt > 5) {
					batchCnt = 1;

					IntSummaryStatistics stat = avgLst.stream().mapToInt((t) -> t)
							.summaryStatistics();

					if (stat.getAverage() < bestTime) {
						bestCntThread = cntThread;
						bestTime = (int) stat.getAverage();
						log.info("Кол-во потоков={}, найдено лучшее время исполнения одного лс={}", bestCntThread, bestTime);
					}
					if (cntThread != 1) {
						cntThread--;
					} else {
						cntThread = bestCntThread;
						setBestCntThread = true;
						log.info("Установлено лучшее кол-во потоков={}", bestCntThread);
					}
					avgLst = new TreeList<>();
				} else {
					batchCnt++;
				}
			}

		}
*/

        endTime = (int) System.currentTimeMillis();
        totalTime = endTime - startTime;
        log.info("Overall time for compress:{} sec", totalTime / 1000);

    }

    /**
     * Сжать все необходимые таблицы
     *
     * @param firstLsk - начать с лиц.сч.
     * @param oneLsk   - только этот лицевой
     */
    @Override
    public boolean comprAllTables(String firstLsk, String oneLsk, String table, boolean isAllPeriods) {
        log.info("**************** СomprAllTables Version 2.0.3 ****************");
        this.isAllPeriods = isAllPeriods;
        // Получить параметры
        // параметры
        Param param = paramDao.findAll().stream().findFirst().orElse(null);
        assert param != null;
        curPeriod = Integer.valueOf(param.getPeriod());
        // Период -2 от текущего (минус два месяца, так как сжимаем только архивный и сравниваем его с доархивным)
        try {
            backPeriod = Integer.valueOf(Utl.addMonths(String.valueOf(curPeriod), -2));
        } catch (ParseException e) {
            e.printStackTrace();
        }
        log.trace("Текущий период={}", curPeriod);
        log.trace("Период -2 от текущего={}", backPeriod);
        try {
            comprTable(table, firstLsk, oneLsk);
        } catch (Exception e) {
            // Ошибка при выполнении
            e.printStackTrace();
            return false;
        }
        return true;
    }


}
