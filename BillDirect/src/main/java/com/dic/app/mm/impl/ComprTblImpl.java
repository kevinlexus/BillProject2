package com.dic.app.mm.impl;

import com.dic.app.mm.ComprTbl;
import com.dic.bill.Compress;
import com.dic.bill.dao.*;
import com.dic.bill.model.scott.Anabor;
import com.ric.cmn.Utl;
import com.ric.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис сжатия таблиц
 *
 * @author lev
 */
@Slf4j
@Service
public class ComprTblImpl implements ComprTbl {

    @PersistenceContext
    private EntityManager em;
    private final AnaborDAO anaborDao;
    private final AchargeDAO achargeDao;
    private final AchargePrepDAO achargePrepDao;
    private final AkartPrDAO akartPrDAO;
    private final ChargePayDAO chargePayDAO;

    public ComprTblImpl(AnaborDAO anaborDao, AchargeDAO achargeDao, AchargePrepDAO achargePrepDao,
                        AkartPrDAO akartPrDAO, ChargePayDAO chargePayDAO) {
        this.anaborDao = anaborDao;
        this.achargeDao = achargeDao;
        this.achargePrepDao = achargePrepDao;
        this.akartPrDAO = akartPrDAO;
        this.chargePayDAO = chargePayDAO;
    }

    /**
     * Сжать таблицу, содержащую mg1, mg2
     *
     * @param table        - таблица для сжатия
     * @param lsk          - лиц.счет
     * @param backPeriod   - текущий период -1 месяц
     * @param curPeriod    - текущий период
     * @param isAllPeriods - использование начального периода:
     *                     (если false, то месяц -1 от текущего, если true - с первого минимального по услуге)
     */
    @Override
    @Transactional
    public void comprTableByLsk(String table, String lsk,
                                Integer backPeriod, Integer curPeriod, boolean isAllPeriods) {
        log.trace("Л.с.:{} Начало сжатия!", lsk);
        log.trace("1.1");
        // isByUsl - использовать ли поле "usl" для критерия сжатия
        log.trace("1.2");
        Result res = new Result(0, lsk);
        log.trace("1.3");
        //lst = new ArrayList<>(1000);
        //log.trace("1.4");

        // Список ключей-услуг
        //Set<String> setKey = new TreeSet<>();

        log.trace("1.5");
        // Минимальный период
        Integer minPeriod;
        log.trace("1.6");
        // Получить все элементы по лиц.счету
        Map<String, List<Compress>> mapElem = new HashMap<>(10);
        switch (table) {
            case "anabor":
                if (isAllPeriods) {
                    // получить все элементы, по всем периодам
                    mapElem = anaborDao.getByLsk(lsk)
                            .stream().collect(Collectors.groupingBy(Anabor::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())
                            ));
                    //lst.addAll(anaborDao.getByLsk(lsk));
                    log.trace("Л.с.:{} По всем периодам Anabor элементы получены!", lsk);
                } else {
                    // начиная с периода -2
                    mapElem = anaborDao.getByLskPeriod(lsk, backPeriod)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    //lst.addAll(anaborDao.getByLskPeriod(lsk, backPeriod));
                    log.trace("Л.с.:{} По по периоду Anabor начиная с -2 элементы получены!", lsk);
                }
                break;
            case "acharge":
                if (isAllPeriods) {
                    // получить все элементы, по всем периодам
                    mapElem = achargeDao.getByLsk(lsk)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    //lst.addAll(achargeDao.getByLsk(lsk));
                    log.trace("Л.с.:{} По всем периодам Acharge элементы получены!", lsk);
                } else {
                    // начиная с периода -2
                    mapElem = achargeDao.getByLskPeriod(lsk, backPeriod)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    //lst.addAll(achargeDao.getByLskPeriod(lsk, backPeriod));
                    log.trace("Л.с.:{} По по периоду Acharge начиная с -2 элементы получены!", lsk);
                }
                break;
            case "achargeprep":
                if (isAllPeriods) {
                    // получить все элементы, по всем периодам
                    mapElem = achargePrepDao.getByLsk(lsk)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    log.trace("Л.с.:{} По всем периодам AchargePrep элементы получены!", lsk);
                } else {
                    // начиная с периода -2
                    mapElem = achargePrepDao.getByLskPeriod(lsk, backPeriod)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    log.trace("Л.с.:{} По по периоду AchargePrep начиная с -2 элементы получены!", lsk);
                }
                break;
            case "akartpr":
                if (isAllPeriods) {
                    // получить все элементы, по всем периодам
                    mapElem = akartPrDAO.getByLsk(lsk)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    //lst.addAll(achargePrepDao.getByLsk(lsk));
                    log.trace("Л.с.:{} По всем периодам AkartPr элементы получены!", lsk);
                } else {
                    // начиная с периода -2
                    mapElem = akartPrDAO.getByLskPeriod(lsk, backPeriod)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    log.trace("Л.с.:{} По по периоду AkartPr начиная с -2 элементы получены!", lsk);
                }
                break;
            case "chargepay":
                if (isAllPeriods) {
                    // получить все элементы, по всем периодам
                    mapElem = chargePayDAO.getByLsk(lsk)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    //lst.addAll(achargePrepDao.getByLsk(lsk));
                    log.trace("Л.с.:{} По всем периодам ChargePay элементы получены!", lsk);
                } else {
                    // начиная с периода -2
                    mapElem = chargePayDAO.getByLskPeriod(lsk, backPeriod)
                            .stream().collect(Collectors.groupingBy(Compress::getKey,
                                    Collectors.mapping((Compress t) -> t, Collectors.toList())));
                    log.trace("Л.с.:{} По по периоду ChargePay начиная с -2 элементы получены!", lsk);
                }
                break;
            default:
                // Ошибка - не тот класс таблицы
                log.error("Л.с.:{} Ошибка! Некорректный класс таблицы!:{}", lsk, table);
                res.setErr(2);
        }

        // Список всех услуг
        //setKey.addAll(lst.stream().map(Compress::getKey).collect(Collectors.toSet()));
        log.trace("Л.с.:{} список найденных ключей:", lsk);
        //setKey.forEach(t-> log.trace("Л.с.:{} ключ:{}", lsk, t));
        mapElem.keySet().forEach(t -> log.trace("Л.с.:{} ключ:{}", lsk, t));
        // периоды последнего обработанного массива
        Integer[] lastUsedPeriod = new Integer[1];
        // массив диапазонов периодов mg1, mg2
        Map<Integer, Integer> lstPeriodPrep;

        // Сжимать с использованием ключа
        for (String key : mapElem.keySet()) {
            lastUsedPeriod[0] = null;
            // Получить все периоды, фильтр - по услуге
            minPeriod = mapElem.get(key).stream()
                    .map(Compress::getMgFrom).min(Integer::compareTo).orElse(null);
            //minPeriod = lst.stream().filter(d -> d.getKey().equals(key))
            //		.map(Compress::getMgFrom).min(Integer::compareTo).orElse(null);
            log.trace("Л.с.:{} мин.период:{}", lsk, minPeriod);

            // Получить все диапазоны периодов mgFrom, mgTo уникальные - по ключу,
            // отсортированные
            lstPeriodPrep = new HashMap<>();

            List<Compress> lst = mapElem.get(key).stream()
                    .filter(t -> t.getMgFrom() < curPeriod).collect(Collectors.toList());
            for (Compress t : lst) {
                if (lstPeriodPrep.get(t.getMgFrom()) == null) {
                    lstPeriodPrep.put(t.getMgFrom(), t.getMgTo());
                }
            }
/*			lst.stream().filter(t -> t.getKey().equals(key) && t.getMgFrom() < curPeriod).forEach(t -> {
				if (lstPeriodPrep.get(t.getMgFrom()) == null) {
					lstPeriodPrep.put(t.getMgFrom(), t.getMgTo());
				}
			});*/

            // отсортированный список периодов
            SortedSet<Integer> lstPeriod = new TreeSet<>(new HashSet<>(lstPeriodPrep.keySet()));

            for (Integer t : lstPeriod) {
                checkPeriod(lstPeriodPrep, mapElem, t, key, lastUsedPeriod);
            }
            // Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
            replacePeriod(mapElem, lastUsedPeriod[0], lstPeriodPrep.get(lastUsedPeriod[0]), key);
        }
        log.trace("Л.с.:{} Окончание сжатия!", lsk);

    }

    /**
     * Проверить массив
     *
     * @param lstPeriodPrep диапазоны периодов
     * @param period         период массива
     * @param key            код услуги
     * @param lastUsedPeriod последний использованный период
     */
    private void checkPeriod(Map<Integer, Integer> lstPeriodPrep, Map<String, List<Compress>> mapElem,
                             Integer period, String key, Integer[] lastUsedPeriod) {
        log.trace("key={} проверяемый.период:{}", key, period);
        Integer lastUsedMg2 = lstPeriodPrep.get(lastUsedPeriod[0]);
        // Период -1 от проверяемого
        Integer chkPeriod = null;
        try {
            chkPeriod = Integer.valueOf(Utl.addMonths(String.valueOf(period), -1));
        } catch (ParseException e) {
            e.printStackTrace();
        }

        if (lastUsedPeriod[0] == null) {
            // последнего периода нет, сохраняем как новый
            lastUsedPeriod[0] = period;
            log.trace("key={} последнего периода нет, сохранили:{}", key, period);
        } else {
            assert chkPeriod != null;
            if (!chkPeriod.equals(lastUsedMg2)) {
                // последний массив есть, но проверяемый период имеет дату начала большую чем на 1 месяц относительно последнего массива (GAP)
                // проставить в заключительном периоде последнего массива замыкающий месяц
                replacePeriod(mapElem, lastUsedPeriod[0], lstPeriodPrep.get(lastUsedPeriod[0]), key);
                lastUsedPeriod[0] = period;
                log.trace("key={} найден GAP:{}", key, period);
            } else {
                // сравнить новый массив с последним
                if (comparePeriod(mapElem, period, lastUsedPeriod[0], key)) {
                    // элементы совпали, удалить элементы сравниваемого массива
                    delPeriod(mapElem, period, key);
                    // Расширить заключительный период последнего массива на mg2 сравниваемого массива
                    lstPeriodPrep.put(lastUsedPeriod[0], lstPeriodPrep.get(period));
                    log.trace("key={} элементы совпали:{}", key, period);
                } else {
                    // элементы разные, закрыть в последнем период действия
                    replacePeriod(mapElem, lastUsedPeriod[0], lstPeriodPrep.get(lastUsedPeriod[0]), key);
                    // пометить период нового массива как замыкающий
                    lastUsedPeriod[0] = period;
                    // сохранять не надо mg2, так как уже записано это при инициализации массива
                    log.trace("key={} элементы разные:{}", key, period);
                }
            }
        }
    }

    /**
     * Удаление элементов массива
     *
     * @param mapelem все периоды
     * @param period  период массива
     * @param key     код ключа
     */
    private void delPeriod(Map<String, List<Compress>> mapelem, Integer period, String key) {
        List<Compress> lstDel = mapelem.get(key).stream()
                .filter(t -> t.getMgFrom().equals(period)).collect(Collectors.toList());
/* note почему то было условие t -> key == null - удалять все записи?
    	List<Compress> lstDel = lst.stream()
    			.filter(t -> key == null || t.getKey().equals(key))
    			.filter(t -> t.getMgFrom().equals(period)).collect(Collectors.toList());
*/

        for (Compress compress : lstDel) {
            em.remove(compress);
        }

    }

    /**
     * Проставить в одном массиве период действия, на другой период
     *
     * @param mapElem Список периодов
     * @param period1 Период расширяемый
     * @param period2 Период новый
     * @param key     код ключа
     */
    private void replacePeriod(Map<String, List<Compress>> mapElem, Integer period1, Integer period2, String key) {
        // Найти массив по period1, и чтобы он еще не был расширен до period2
        mapElem.get(key).stream()
                .filter(t -> t.getMgFrom().equals(period1) && !t.getMgTo().equals(period2)).forEach(d -> d.setMgTo(period2));
/*
		lst.stream()
			.filter(t -> key == null || t.getKey().equals(key)) note было условие key == null - удалять все записи?
		    .filter(t -> t.getMgFrom().equals(period1) && !t.getMgTo().equals(period2)).forEach(d -> d.setMgTo(period2));
*/
    }

    /**
     * Сравнить элементы одного массива с другим
     *
     * @param mapElem все периоды
     * @param period1 период 1
     * @param period2 период 2
     * @param key     код ключа
     */
    private boolean comparePeriod(Map<String, List<Compress>> mapElem, Integer period1, Integer period2, String key) {
        List<Compress> filtLst1 = mapElem.get(key).stream()
                .filter(t -> t.getMgFrom().equals(period1)).collect(Collectors.toList());
        List<Compress> filtLst2 = mapElem.get(key).stream()
                .filter(t -> t.getMgFrom().equals(period2)).collect(Collectors.toList());

        if (filtLst1.size() != filtLst2.size()) {
            // не равны по размеру
            log.trace("Не равны по размеру!");
            return false;
        }
        // Сравнить своим компаратором
        Eq equator = new Eq();
        return CollectionUtils.isEqualCollection(filtLst1, filtLst2, equator);
    }

    public static class Eq implements Equator<Compress> {

        @Override
        public boolean equate(Compress o1, Compress o2) {

            return o1.isTheSame(o2);
        }

        // используется метод getHash, так как надо сравнивать только выборочные поля,
        // иначе - будет некорректное сравнение строк и не произойдёт сжатие таблиц
        @Override
        public int hash(Compress o) {
            return o.getHash();
        }

    }
}
