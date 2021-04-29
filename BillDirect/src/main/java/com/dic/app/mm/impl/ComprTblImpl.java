package com.dic.app.mm.impl;

import com.dic.app.mm.ComprTbl;
import com.dic.bill.Compress;
import com.dic.bill.dao.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.dto.Result;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.collections4.Equator;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.text.ParseException;
import java.util.*;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

/**
 * Сервис сжатия таблиц
 * @author lev
 *
 */
@Slf4j
@Service
@Scope("prototype")
public class ComprTblImpl implements ComprTbl {

    @PersistenceContext
    private EntityManager em;
	private final AnaborDAO anaborDao;
	private final AchargeDAO achargeDao;
	private final AchargePrepDAO achargePrepDao;
	private final AkartPrDAO akartPrDAO;
	private final ChargePayDAO chargePayDAO;

	// Все элементы по лиц.счету по всем периодам
	private Map<String, List<Compress>> mapElem = new HashMap<>(10);
	//List<Compress> lst;
	// периоды последнего обработанного массива
	private Integer lastUsed;
	// массив диапазонов периодов mg1, mg2
	private Map<Integer, Integer> lstPeriodPrep;
	// текущий л.с.
	String lsk;

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
	 * @param table - таблица для сжатия
	 * @param lsk - лиц.счет
	 * @param backPeriod - текущий период -1 месяц
	 * @param curPeriod - текущий период
	 * @param isAllPeriods - использование начального периода:
*    (если false, то месяц -1 от текущего, если true - с первого минимального по услуге)
	 */
	@Override
	@Async //- Async чтобы организовался поток
    @Transactional
	public Future<Result> comprTableByLsk(String table, String lsk,
										  Integer backPeriod, Integer curPeriod, boolean isAllPeriods) {
		log.info("Л.с.:{} Начало сжатия!", lsk);
		this.lsk = lsk;
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
				return new AsyncResult<>(res);
		}

    	// Список всех услуг
    	//setKey.addAll(lst.stream().map(Compress::getKey).collect(Collectors.toSet()));
		log.trace("Л.с.:{} список найденных ключей:", lsk);
		//setKey.forEach(t-> log.trace("Л.с.:{} ключ:{}", lsk, t));
		mapElem.keySet().forEach(t-> log.trace("Л.с.:{} ключ:{}", lsk, t));
		// Сжимать с использованием ключа
		for (String key : mapElem.keySet()) {
			lastUsed = null;
			// Получить все периоды, фильтр - по услуге
			minPeriod = mapElem.get(key).stream()
					.map(Compress::getMgFrom).min(Integer::compareTo).orElse(null);
			//minPeriod = lst.stream().filter(d -> d.getKey().equals(key))
			//		.map(Compress::getMgFrom).min(Integer::compareTo).orElse(null);
			log.trace("Л.с.:{} мин.период:{}", lsk, minPeriod);

			// Получить все диапазоны периодов mgFrom, mgTo уникальные - по ключу,
			// отсортированные
			lstPeriodPrep = new HashMap<>();

			mapElem.get(key).stream().filter(t -> t.getMgFrom() < curPeriod).forEach(t -> {
				if (lstPeriodPrep.get(t.getMgFrom()) == null) {
					lstPeriodPrep.put(t.getMgFrom(), t.getMgTo());
				}
			});
/*			lst.stream().filter(t -> t.getKey().equals(key) && t.getMgFrom() < curPeriod).forEach(t -> {
				if (lstPeriodPrep.get(t.getMgFrom()) == null) {
					lstPeriodPrep.put(t.getMgFrom(), t.getMgTo());
				}
			});*/

			// отсортированный список периодов
			SortedSet<Integer> lstPeriod = new TreeSet<>(new HashSet<>(lstPeriodPrep.keySet()));

			lstPeriod.forEach(t -> checkPeriod(t, key));

			// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
			replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
		}
		log.info("Л.с.:{} Окончание сжатия!", lsk);

		return new AsyncResult<>(res);
    }

    /**
     * Проверить массив
     * @param period - период массива
     * @param key - код услуги
     */
	private void checkPeriod(Integer period, String key) {
		log.trace("Л.с.:{}, key={} проверяемый.период:{}", this.lsk, key, period);
		Integer lastUsedMg2 = lstPeriodPrep.get(lastUsed);
		// Период -1 от проверяемого
		Integer chkPeriod = null;
		try {
			chkPeriod = Integer.valueOf(Utl.addMonths(String.valueOf(period),-1));
		} catch (ParseException e) {
			e.printStackTrace();
		}

		if (lastUsed == null) {
    		// последнего массива нет, сохраняем как новый
    		lastUsed = period;
    		log.trace("Л.с.:{}, key={} последнего периода нет, сохранили:{}", this.lsk, key, period);
    	} else {
			assert chkPeriod != null;
			if (!chkPeriod.equals(lastUsedMg2)) {
				// последний массив есть, но проверяемый период имеет дату начала большую чем на 1 месяц относительно последнего массива (GAP)
				// проставить в заключительном периоде последнего массива замыкающий месяц
				replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
				lastUsed = period;
				log.trace("Л.с.:{}, key={} найден GAP:{}", this.lsk, key, period);
			} else {
				// сравнить новый массив с последним
				if (comparePeriod(period, lastUsed, key)) {
					// элементы совпали, удалить элементы сравниваемого массива
					delPeriod(period, key);
					// Расширить заключительный период последнего массива на mg2 сравниваемого массива
					lstPeriodPrep.put(lastUsed, lstPeriodPrep.get(period));
					log.trace("Л.с.:{}, key={} элементы совпали:{}", this.lsk, key, period);
				} else {
					// элементы разные, закрыть в последнем период действия
					replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
					// пометить период нового массива как замыкающий
					lastUsed = period;
					// сохранять не надо mg2, так как уже записано это при инициализации массива
					log.trace("Л.с.:{}, key={} элементы разные:{}", this.lsk, key, period);
				}
			}
		}
    }

	/**
	 * Удаление элементов массива
	 * @param period - период массива
     * @param key - код ключа
	 */
    private void delPeriod(Integer period, String key) {
    	List<Compress> lstDel = mapElem.get(key).stream()
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
     * @param period1 - Период расширяемый
     * @param period2 - Период новый
     * @param key - код ключа
     */
    private void replacePeriod(Integer period1, Integer period2, String key) {
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
     * @param period1 - Период 1
     * @param period2 - Период 2
     * @param key - код ключа
     */
    private boolean comparePeriod(Integer period1, Integer period2, String key) {
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
