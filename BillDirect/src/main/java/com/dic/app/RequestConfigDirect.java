package com.dic.app;

import com.dic.app.service.ConfigApp;
import com.dic.app.service.impl.enums.ProcessTypes;
import com.dic.bill.SpringContext;
import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.SaldoUslDAO;
import com.dic.bill.dao.VvodDAO;
import com.dic.bill.dto.ChrgCountAmount;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.dic.app.service.impl.enums.ProcessTypes.*;


/**
 * Конфигуратор запроса
 *
 * @author Lev
 */
@Getter
@Setter
@Slf4j
public class RequestConfigDirect implements Cloneable {


    private CalcScope calcScope;

    // тип объектов
    enum CalcScope {
        PREMISE,
        UK,
        HOUSE,
        KULNDS,
        KLSK_IDS,
        VVOD,
        ALL,
        NOT_SPECIFIED
    }

    // Id запроса
    int rqn;
    // тип выполнения
    ProcessTypes tp = CHARGE_0;
    // уровень отладки
    int debugLvl = 0;
    // дата на которую сформировать
    Date genDt = null;

    // текущий период
    Date curDt1;
    Date curDt2;

    // выполнять многопоточно
    boolean isMultiThreads = true;
    // кол-во потоков (по умолчанию =1)
    int cntThreads = 1;
    //final int CNT_THREADS_FOR_COMMON_TASKS = Runtime.getRuntime().availableProcessors(); todo временно убрал, у Полыс стало тормозить
    final int CNT_THREADS_FOR_COMMON_TASKS = 15;
    // кол-во потоков для начисления по распределению объемов
    //final int CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS = Runtime.getRuntime().availableProcessors();todo временно убрал, у Полыс стало тормозить
    final int CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS = 20;
    // объекты формирования:
    // УК
    Org uk = null;
    // дом
    House house = null;
    // ввод
    Vvod vvod = null;
    // помещение
    Ko ko = null;
    // список кодов улиц+домов (kul+nd)
    List<String> kulNds;
    // список фин.лиц.сч.
    List<Long> klskIds;
    // начальный и конечный лиц.счет
    String lskFrom;
    String lskTo;

    // блокировать для выполнения длительного процесса?
    private boolean isLockForLongLastingProcess = false;
    // задан расчет одного объекта? (используется в логгировании)
    private boolean isSingleObjectCalc = false;

    // маркер остановки формирования
    private String stopMark;

    // услуга
    Usl usl = null;

    // список Id объектов формирования
    List<Long> lstItems = new ArrayList<>(0);

    // список String объектов формирования
    List<String> lstStrItems;

    // хранилище объемов по вводу (дому) (для ОДН и прочих нужд) - работает только в начислении для распределения объема
    private ChrgCountAmount chrgCountAmount;

    // сохранять результаты расчета в БД
    boolean saveResult = true;

    public Object clone() throws CloneNotSupportedException {
        return super.clone();
    }

    // получить наименование типа выполнения
    public String getTpName() {
        return switch (this.tp) {
            case CHARGE_0 -> "Начисление";
            case DEBT_PEN_1 -> "Задолженность и пеня";
            case DIST_VOL_2 -> "Распределение объемов";
            case CHARGE_FOR_DIST_3 -> "Начисление для распределения объемов";
            case CHARGE_SINGLE_USL_4 -> "Начисление по одной услуге, для автоначисления";
            case MIGRATION_5 -> "Миграция долгов";
        };
    }

    /**
     * Проверить параметры запроса
     *
     * @return - null -  нет ошибок, !null - описание ошибки
     */
    public String checkArguments() {
        switch (this.tp) {
            case DEBT_PEN_1, DIST_VOL_2, CHARGE_FOR_DIST_3, CHARGE_SINGLE_USL_4 -> {
                // задолженность и пеня, - проверить текущую дату
                if (genDt == null) {
                    return "ERROR! некорректная дата расчета!";
                } else {
                    // проверить, что дата в диапазоне текущего периода
                    if (!Utl.between(genDt, curDt1, curDt2)) {
                        return "ERROR! дата не находится в текущем периоде genDt=" + genDt;
                    }
                }
            }
        }
        if (this.tp == CHARGE_SINGLE_USL_4) {
            if (this.usl == null) {
                return "ERROR! не заполнена услуга (uslId) для расчета!";
            }
        }

        return null;
    }


    /**
     * Подготовка списка Id (помещений, вводов)
     */
    public void prepareId() {
        calcScope = CalcScope.NOT_SPECIFIED;
        if (Utl.in(tp, CHARGE_0, DEBT_PEN_1, CHARGE_FOR_DIST_3, CHARGE_SINGLE_USL_4)) {
            // начисление, начисление для распределения объемов, начисление по одной услуге, для автоначисления
            KartDAO kartDao = SpringContext.getBean(KartDAO.class);
            if (ko != null) {
                // по помещению
                calcScope = CalcScope.PREMISE;
                isLockForLongLastingProcess = false;
                lstItems = new ArrayList<>(1);
                lstItems.add(ko.getId());
                cntThreads = 1;
                isSingleObjectCalc = true;
            } else if (uk != null) {
                // по УК
                calcScope = CalcScope.UK;
                isLockForLongLastingProcess = true;
                lstItems = kartDao.findAllKlskIdByReuId(uk.getReu())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            } else if (house != null) {
                // по дому
                calcScope = CalcScope.HOUSE;
                isLockForLongLastingProcess = false;
                lstItems = kartDao.findAllKlskIdByHouseId(house.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            } else if (!CollectionUtils.isEmpty(kulNds)) {
                // по списку домов в виде kul+nd
                calcScope = CalcScope.KULNDS;
                isLockForLongLastingProcess = true;
                lstItems = kartDao.findAllKlskIdByKulNds(kulNds)
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            } else if (!CollectionUtils.isEmpty(klskIds)) {
                // по списку фин.лиц.сч.
                calcScope = CalcScope.KLSK_IDS;
                isLockForLongLastingProcess = true;
                lstItems = klskIds;
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            } else if (vvod != null) {
                // по вводу
                calcScope = CalcScope.VVOD;
                isLockForLongLastingProcess = false;
                lstItems = kartDao.findAllKlskIdByVvodId(vvod.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            } else {
                // по всему фонду
                calcScope = CalcScope.ALL;
                isLockForLongLastingProcess = true;
                // конвертировать из List<BD> в List<Long> (native JPA представляет k_lsk_id только в BD и происходит type Erasure)
                lstItems = kartDao.findAllKlskId()
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                if (tp == CHARGE_FOR_DIST_3) {
                    // кол-во потоков для начисления по распределению объемов
                    cntThreads = CNT_THREADS_FOR_CHARGE_FOR_DIST_VOLS;
                } else {
                    cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
                }
            }
        } else if (tp == DIST_VOL_2) {
            // распределение вводов
            VvodDAO vvodDAO = SpringContext.getBean(VvodDAO.class);
            if (vvod != null) {
                // распределить конкретный ввод
                calcScope = CalcScope.VVOD;
                lstItems = new ArrayList<>(1);
                lstItems.add(vvod.getId());
                cntThreads = 1;
            } else if (uk != null) {
                // по УК
                calcScope = CalcScope.UK;
                lstItems = vvodDAO.findVvodByReu(uk.getReu())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            } else if (house != null) {
                // по дому
                calcScope = CalcScope.HOUSE;
                lstItems = vvodDAO.findVvodByHouse(house.getId())
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = 1;
            } else {
                // все вводы
                calcScope = CalcScope.ALL;
                isLockForLongLastingProcess = true;
                lstItems = vvodDAO.findVvodAll()
                        .stream().map(BigDecimal::longValue).collect(Collectors.toList());
                cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            }
        } else if (tp == MIGRATION_5) {
            // миграция долгов
            ConfigApp config = SpringContext.getBean(ConfigApp.class);
            SaldoUslDAO saldoUslDao = SpringContext.getBean(SaldoUslDAO.class);
            cntThreads = CNT_THREADS_FOR_COMMON_TASKS;
            lstStrItems = saldoUslDao.getAllWithNonZeroDeb(lskFrom, lskTo,
                    config.getPeriodBack());
        } else {
            throw new IllegalArgumentException("Параметр tp=" + tp + " не обслуживается методом");
        }
        log.info("Для процесса tp={} будет использовано {} кол-во ядер", tp, cntThreads);
    }

    /**
     * Подготовка хранилища для расчета ОДН
     */
    public void prepareChrgCountAmount() {
        // хранилище параметров по дому (для ОДН и прочих нужд)
        setChrgCountAmount(new ChrgCountAmount());
    }

    // доля одного дня в периоде
    public BigDecimal getPartDayMonth() {
        BigDecimal oneDay = new BigDecimal("1");
        BigDecimal monthDays = BigDecimal.valueOf(Utl.getCntDaysByDate(getCurDt1()));
        return oneDay.divide(monthDays, 20, RoundingMode.HALF_UP);
    }

    // получить следующий id, для расчета в потоках
    public synchronized Long getNextItem() {
        Iterator<Long> itr = lstItems.iterator();
        Long item = null;
        if (itr.hasNext()) {
            item = itr.next();
            itr.remove();
        }
        return item;
    }

    // получить следующий String идентификатор, для расчета в потоках
    public synchronized String getNextStrItem() {
        Iterator<String> itr = lstStrItems.iterator();
        String item = null;
        if (itr.hasNext()) {
            item = itr.next();
            itr.remove();
        }
        return item;
    }

    public static final class RequestConfigDirectBuilder {
        // Id запроса
        int rqn;
        // тип выполнения 0-начисление, 3-начисление для распределения по вводу, 1-задолженность и пеня, 2 - распределение объемов по вводу,
        // 4 - начисление по одной услуге, для автоначисления
        ProcessTypes tp = CHARGE_0;
        // уровень отладки
        int debugLvl = 0;
        // дата на которую сформировать
        Date genDt = null;
        // текущий период
        Date curDt1;
        Date curDt2;
        // выполнять многопоточно
        boolean isMultiThreads = false;
        // объекты формирования:
        // УК
        Org uk = null;
        // дом
        House house = null;
        // список кодов улиц+домов (kul+nd)
        List<String> kulNds;
        // список фин.лиц.сч.
        List<Long> klskIds;
        // ввод
        Vvod vvod = null;
        // помещение
        Ko ko = null;

        // начальный и конечный лиц.счет
        String lskFrom;
        String lskTo;

        // услуга
        Usl usl = null;
        // список Id объектов формирования
        List<Long> lstItems;
        // маркер остановки формирования
        String stopMark;
        // сохранять результаты расчета в БД
        boolean saveResult = true;

        private RequestConfigDirectBuilder() {
        }

        public static RequestConfigDirectBuilder aRequestConfigDirect() {
            return new RequestConfigDirectBuilder();
        }

        public RequestConfigDirectBuilder withRqn(int rqn) {
            this.rqn = rqn;
            return this;
        }

        public RequestConfigDirectBuilder withTp(ProcessTypes tp) {
            this.tp = tp;
            return this;
        }

        public RequestConfigDirectBuilder withDebugLvl(int debugLvl) {
            this.debugLvl = debugLvl;
            return this;
        }

        public RequestConfigDirectBuilder withGenDt(Date genDt) {
            this.genDt = genDt;
            return this;
        }

        public RequestConfigDirectBuilder withCurDt1(Date curDt1) {
            this.curDt1 = curDt1;
            return this;
        }

        public RequestConfigDirectBuilder withCurDt2(Date curDt2) {
            this.curDt2 = curDt2;
            return this;
        }

        public RequestConfigDirectBuilder withIsMultiThreads(boolean isMultiThreads) {
            this.isMultiThreads = isMultiThreads;
            return this;
        }

        public RequestConfigDirectBuilder withUk(Org uk) {
            this.uk = uk;
            return this;
        }

        public RequestConfigDirectBuilder withHouse(House house) {
            this.house = house;
            return this;
        }

        public RequestConfigDirectBuilder withKulNds(List<String> kulNds) {
            this.kulNds = kulNds;
            return this;
        }

        public RequestConfigDirectBuilder withKlskIds(List<Long> klskIds) {
            this.klskIds = klskIds;
            return this;
        }

        public RequestConfigDirectBuilder withVvod(Vvod vvod) {
            this.vvod = vvod;
            return this;
        }

        public RequestConfigDirectBuilder withKo(Ko ko) {
            this.ko = ko;
            return this;
        }

        public RequestConfigDirectBuilder withUsl(Usl usl) {
            this.usl = usl;
            return this;
        }

        public RequestConfigDirectBuilder withLskFrom(String lskFrom) {
            this.lskFrom = lskFrom;
            return this;
        }

        public RequestConfigDirectBuilder withLskTo(String lskTo) {
            this.lskTo = lskTo;
            return this;
        }

        public RequestConfigDirectBuilder withStopMark(String stopMark) {
            this.stopMark = stopMark;
            return this;
        }

        public RequestConfigDirectBuilder withsaveResult(boolean saveResult) {
            this.saveResult = saveResult;
            return this;
        }

        // строить не builder-ом!!!
        public RequestConfigDirect build() {
            RequestConfigDirect requestConfigDirect = new RequestConfigDirect();
            requestConfigDirect.setRqn(rqn);
            requestConfigDirect.setTp(tp);
            requestConfigDirect.setDebugLvl(debugLvl);
            requestConfigDirect.setGenDt(genDt);
            requestConfigDirect.setCurDt1(curDt1);
            requestConfigDirect.setCurDt2(curDt2);
            requestConfigDirect.setUk(uk);
            requestConfigDirect.setHouse(house);
            requestConfigDirect.setKulNds(kulNds);
            requestConfigDirect.setKlskIds(klskIds);
            requestConfigDirect.setVvod(vvod);
            requestConfigDirect.setKo(ko);
            requestConfigDirect.setLskFrom(lskFrom);
            requestConfigDirect.setLskTo(lskTo);

            requestConfigDirect.setUsl(usl);
            requestConfigDirect.setLstItems(lstItems);
            requestConfigDirect.setStopMark(stopMark);
            requestConfigDirect.isMultiThreads = this.isMultiThreads;

            return requestConfigDirect;
        }
    }
}
