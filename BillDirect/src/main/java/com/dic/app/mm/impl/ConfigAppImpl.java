package com.dic.app.mm.impl;

import com.dic.app.mm.ConfigApp;
import com.dic.bill.Lock;
import com.dic.bill.dao.TuserDAO;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.mm.ParMng;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.mm.impl.SprParamMngImpl;
import com.dic.bill.model.scott.SprPen;
import com.dic.bill.model.scott.Stavr;
import com.dic.bill.model.scott.Tuser;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManager;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Конфигуратор приложения
 *
 * @author lev
 * @version 1.01
 */
@Service
@Slf4j
@Getter
@Setter
public class ConfigAppImpl implements ConfigApp {

    private final ApplicationContext ctx;
    private final EntityManager em;
    private final SprParamMng sprParamMng;
    private final ParMng parMng;
    private final TuserDAO tuserDAO;

    // номер текущего запроса
    private int reqNum = 0;

    // прогресс текущего формирования
    private Integer progress;

    // блокировщик выполнения процессов
    private Lock lock;
    // справочник дат начала пени
    private List<SprPen> lstSprPen;
    // справочник ставок рефинансирования
    private List<Stavr> lstStavr;
    // справочник дат начала пени
    private Map<SprPenKey, SprPen> mapSprPen = new ConcurrentHashMap<>();
    // справочник дат
    private Map<String, Date> mapDate = new ConcurrentHashMap<>();
    // справочник булевых параметров
    private Map<String, Boolean> mapParams = new ConcurrentHashMap<>();

    public ConfigAppImpl(ApplicationContext ctx, EntityManager em, SprParamMng sprParamMng, ParMng parMng, TuserDAO tuserDAO) {
        this.ctx = ctx;
        this.em = em;
        this.sprParamMng = sprParamMng;
        this.parMng = parMng;
        this.tuserDAO = tuserDAO;
    }

    @PostConstruct
    private void setUp() {
        log.info("");
        log.info("-----------------------------------------------------------------");
        log.info("Версия модуля - {}", "1.2.0");

        reloadSprPen();
        reloadParam();
        log.info("-----------------------------------------------------------------");
        log.info("");

        //TimeZone.setDefault(TimeZone.getTimeZone("GMT+7"));
        //log.info("************** Установлена TIMEZONE");
        // блокировщик процессов
        setLock(new Lock());
    }

    /**
     * Проверка необходимости выйти из приложения
     *
     * @Scheduled(fixedDelay = 2000)
     * public void checkTerminate() {
     * // проверка файла "stop" на завершение приложения (для обновления)
     * File tempFile = new File("stop");
     * boolean exists = tempFile.exists();
     * if (exists) {
     * log.info("ВНИМАНИЕ! ЗАПРОШЕНА ОСТАНОВКА ПРИЛОЖЕНИЯ! - БЫЛ СОЗДАН ФАЙЛ c:\\Progs\\GisExchanger\\stop");
     * SpringApplication.exit(ctx, () -> 0);
     * }
     * }
     */

    @Override
    public String getPeriod() {
        return Utl.getPeriodFromDate(mapDate.get("dtFirst"));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Tuser getCurUser() {
        return tuserDAO.findByCd("GEN");
    }

    @Override
    public String getPeriodNext() {
        try {
            return Utl.addMonths(Utl.getPeriodFromDate(mapDate.get("dtFirst")), 1);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            return null;
        }
    }

    @Override
    public String getPeriodBack() {
        try {
            return Utl.addMonths(Utl.getPeriodFromDate(mapDate.get("dtFirst")), -1);
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            return null;
        }
    }

    /**
     * Получить первую дату текущего месяца
     */
    @Override
    public Date getCurDt1() {
        return mapDate.get("dtFirst");
    }

    /**
     * Получить последнюю дату текущего периода
     */
    @Override
    public Date getCurDt2() {
        return mapDate.get("dtLast");
    }

    // Получить дату середины месяца
    @Override
    public Date getDtMiddleMonth() {
        return mapDate.get("dtMiddle");
    }

    // получить следующий номер запроса
    @Override
    public synchronized int incNextReqNum() {
        return this.reqNum++;
    }

    @Override
    public Lock getLock() {
        return lock;
    }

    private void setLock(Lock lock) {
        this.lock = lock;
    }

    @Override
    public Integer getProgress() {
        return progress;
    }

    @Override
    public void setProgress(Integer progress) {
        this.progress = progress;
    }

    @Override
    public void incProgress() {
        progress++;
    }

    @Override
    public void reloadSprPen() {
        SprParamMngImpl.StavPen stavPen = sprParamMng.getStavPen();
        lstSprPen = stavPen.getLstSprPen();
        lstStavr = stavPen.getLstStavr();
        mapSprPen = stavPen.getMapSprPen();
    }

    @Override
    public void reloadParam() {
        parMng.reloadParam(mapDate, mapParams);
    }

}
