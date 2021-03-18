package com.dic.app.mm.impl;

import com.dic.app.mm.DebitByLskThrMng;
import com.dic.app.mm.GenPenMng;
import com.dic.bill.dao.*;
import com.dic.bill.dto.*;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.PenCur;
import com.dic.bill.model.scott.Penya;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrgPen;
import lombok.Getter;
import lombok.Setter;
import lombok.Value;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.toMap;

/**
 * Сервис обработки строк задолженности и расчета пени по дням
 *
 * @author lev
 * @version 1.18
 */
@Slf4j
@Service
public class DebitByLskThrMngImpl implements DebitByLskThrMng {

    @PersistenceContext
    private EntityManager em;
    private final GenPenMng genPenMng;
    private final PenCurDAO penCurDAO;
    private final PenyaDAO penDAO;
    private final ApenyaDAO apenyaDAO;
    private final PenCorrDAO penCorrDAO;
    private final KwtpMgDAO kwtpMgDAO;


    public DebitByLskThrMngImpl(EntityManager em, GenPenMng genPenMng, PenCurDAO penCurDAO,
                                PenyaDAO penDAO, ApenyaDAO apenyaDAO, PenCorrDAO penCorrDAO, KwtpMgDAO kwtpMgDAO) {
        this.em = em;
        this.genPenMng = genPenMng;
        this.penCurDAO = penCurDAO;
        this.penDAO = penDAO;
        this.apenyaDAO = apenyaDAO;
        this.penCorrDAO = penCorrDAO;
        this.kwtpMgDAO = kwtpMgDAO;
    }

    @Getter
    @Setter
    class PeriodSumma {
        // задолженность
        private BigDecimal deb;
        // задолженность для расчета пени
        private BigDecimal debForPen;

        PeriodSumma(BigDecimal deb, BigDecimal debForPen) {
            this.deb = deb;
            this.debForPen = debForPen;
        }
    }


    /**
     * Свернуть, рассчитать задолженность, выполнить расчет пени
     *
     * @param kart       - лиц.счет
     * @param calcStore  - хранилище справочников
     * @param localStore - хранилище всех операций по лиц.счету
     */
    @Override
    @Transactional(
            propagation = Propagation.REQUIRED,
            rollbackFor = Exception.class)
    public void genDebPen(Kart kart, CalcStore calcStore,
                          CalcStoreLocal localStore) throws ErrorWhileChrgPen {
        @Value
        class TempSumRec implements SumRecMgDt {
            BigDecimal summa;
            Integer mg;
            Date dt;
            Integer tp;
        }
        // дата начала расчета
        Date dt1 = calcStore.getCurDt1();
        // дата окончания расчета
        Date dt2 = calcStore.getGenDt();

        // долги предыдущего периода (вх.сальдо)
        Map<Integer, PeriodSumma> mapDebPart1 = new HashMap<>();
        localStore.getLstDebFlow()
                .forEach(t -> mapDebPart1.put(t.getMg(),
                        new PeriodSumma(t.getDebOut(), t.getDebOut())));

        // добавить текущий период, для корректного сворачивания переплаты
        mapDebPart1.putIfAbsent(calcStore.getPeriod(), new PeriodSumma(BigDecimal.ZERO, BigDecimal.ZERO));

        // добавить текущее начисление
        BigDecimal chrgSumm = localStore.getChrgSum();
        List<SumRecMgDt> lst = new ArrayList<>();
        lst.add(new TempSumRec(chrgSumm, null, dt1, null));
        process(lst.stream(), mapDebPart1, null, null, false, calcStore.getPeriod());

        // обновить mgTo записей, если они были расширены до текущего периода
        //debDAO.delByLskPeriod(kart.getLsk(), calcStore.getPeriod());
        //debDAO.updByLskPeriod(kart.getLsk(), calcStore.getPeriod(), calcStore.getPeriodBack());

        HashMap<Integer, PeriodSumma> mapDebPart2;
        // свернутые долги для расчета пени по дням - совокупно все услуги - упорядоченный по ключу (дата) LinkedHashMap
        Map<Date, Map<Integer, BigDecimal>> mapDebForPen = new LinkedHashMap<>();
        // долги на последнюю дату - совокупно все услуги
        Map<Integer, BigDecimal> mapDeb = new LinkedHashMap<>();

        // перебрать все дни с начала месяца по дату расчета, включительно
        Calendar c = Calendar.getInstance();
        for (c.setTime(dt1); !c.getTime().after(dt2); c.add(Calendar.DATE, 1)) {
            Date dt = c.getTime();

            // восстановить неизменную часть
            mapDebPart2 =
                    mapDebPart1.entrySet().stream().collect(toMap(
                            Map.Entry::getKey,
                            v -> new PeriodSumma(v.getValue().getDeb(), v.getValue().getDebForPen()),
                            (k, v) -> k, HashMap::new));

            // перерасчеты, включая текущий день
            lst = localStore.getLstChngFlow().stream()
                    .map(t -> new TempSumRec(t.getSumma(), t.getMg(), dt1, null))
                    .collect(Collectors.toList());
            process(lst.stream(), mapDebPart2, dt, null, false, null);

            // вычесть оплату включая текущий день поступления - для обычного долга
            // и не включая для расчета пени
            lst = localStore.getLstPayFlow().stream()
                    .map(t -> new TempSumRec(t.getSumma(), t.getMg(), t.getDt(), null))
                    .collect(Collectors.toList());
            process(lst.stream(), mapDebPart2, dt, dt, true, null);

            // вычесть корректировки оплаты - для расчета долга, включая текущий день
            lst = localStore.getLstPayCorrFlow().stream()
                    .map(t -> new TempSumRec(t.getSumma(), t.getMg(), dt1, null))
                    .collect(Collectors.toList());
            process(lst.stream(), mapDebPart2, dt, null, true, null);

            log.trace("********** Долги на дату: dt={}, lsk={}", Utl.getStrFromDate(dt), kart.getLsk());
            mapDebPart2.forEach((key, value) -> {
                log.trace("долг: mg={}, deb={}, debForPen={}",
                        key, value.getDeb(), value.getDebForPen());
            });

            // перенести переплату
            moveOverpay(mapDebPart2);

            mapDebPart2.entrySet().stream().sorted((Comparator.comparing(o -> o.getKey())))
                    .forEach(t -> {
                        log.trace("Свернуто: mg={}, deb={}, debForPen={}",
                                t.getKey(),
                                t.getValue().getDeb(), t.getValue().getDebForPen());
                    });

            // сгруппировать сумму свернутых долгов для расчета пени по всем услугам, по датам
            groupByDateMg(mapDebPart2, mapDebForPen, dt);

            if (dt.getTime() == dt2.getTime()) {
                // сгруппировать сумму свернутых основных долгов по всем услугам, на последнюю дату
                groupByDateMg(mapDebPart2, mapDeb);
            }

        }
        // рассчитать и сохранить пеню
        genSavePen(kart, calcStore, mapDebForPen, mapDeb);
    }


    /**
     * Сохранить запись долга fixme удалить потом ред.09.12.20
     *  @param calcStore  - хранилище справочников
     * @param kart       - лиц.счет
     * @param localStore - хранилище всех операций по лиц.счету
     * @param uslId      - ID услуги
     * @param orgId      - ID организации
     * @param mg         - период
     * @param periodSumma     - долг
     */
/*
    private void saveDeb(CalcStore calcStore, Kart kart, CalcStoreLocal localStore, String uslId,
                         int orgId, int mg, PeriodSumma periodSumma) {
        // флаг создания новой записи
        boolean isCreate = false;
        // найти запись долгов предыдущего периода
        SumDebPenRec foundDeb = localStore.getLstDebFlow().stream()
                .filter(d -> d.getUslId().equals(uslId))
                .filter(d -> d.getOrgId().equals(orgId))
                .filter(d -> d.getMg().equals(mg))
                .findFirst().orElse(null);
        if (foundDeb == null) {
            // не найдена, создать новую запись
            isCreate = true;
        } else {
            // найдена, проверить равенство по полям
            if (periodSumma.getDeb().compareTo(foundDeb.getDebOut()) == 0 &&
                    periodSumma.getDebForPen().compareTo(foundDeb.getDebRolled()) == 0) {
                // равны, расширить период
                Deb deb = em.find(Deb.class, foundDeb.getId());
                deb.setMgTo(calcStore.getPeriod());
            } else {
                // не равны, создать запись нового периода
                isCreate = true;
            }
        }
        if (isCreate) {
            // создать запись нового периода
            if (periodSumma.getDeb().compareTo(BigDecimal.ZERO) != 0 ||
                    periodSumma.getDebForPen().compareTo(BigDecimal.ZERO) != 0) {
                // если хотя бы одно поле != 0
                Usl usl = em.find(Usl.class, uslId);
                if (usl == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена услуга с кодом usl=" + uslId);
                }
                Org org = em.find(Org.class, orgId);
                if (org == null) {
                    // так как внутри потока, то только RuntimeException
                    throw new RuntimeException("Ошибка при сохранении записей долгов,"
                            + " некорректные данные в таблице SCOTT.DEB!"
                            + " Не найдена организация с кодом org=" + orgId);
                }
                Deb deb = Deb.builder()
                        .withUsl(usl)
                        .withOrg(org)
                        .withDebOut(periodSumma.getDeb())
                        .withDebRolled(periodSumma.getDebForPen())
                        .withKart(kart)
                        .withMgFrom(calcStore.getPeriod())
                        .withMgTo(calcStore.getPeriod())
                        .withMg(mg)
                        .build();
                kart.getDeb().add(deb);
                em.persist(deb);
            }
        }
    }
*/

    /**
     * Рассчитать пеню
     *
     * @param kart         - текущий лиц.счет
     * @param calcStore    - хранилище справочников
     * @param mapDebForPen - долги для расчета пени
     * @param mapDeb       - долги
     */
    private void genSavePen(Kart kart, CalcStore calcStore, Map<Date,
            Map<Integer, BigDecimal>> mapDebForPen, Map<Integer, BigDecimal> mapDeb) throws ErrorWhileChrgPen {
        // расчитать пеню по долгам
        // пеня для C_PEN_CUR
        List<PenCurRec> lstPenCurRec = new ArrayList<>(10);

        // последние элементы записей пени
        Map<Integer, PenCurRec> mapLastPenCurRec = new HashMap<>();

        // для записи в C_PENYA:
        // кол-во дней долга, на последнюю дату расчета
        Map<Integer, Integer> mapDebDays = new HashMap<>();

        // перебрать все элементы долгов для пени mapDebForPen, (LinkedHashMap - отсортированный по дате)
        for (Map.Entry<Date, Map<Integer, BigDecimal>> dtEntry : mapDebForPen.entrySet()) {
            Date dt = dtEntry.getKey();
            for (Map.Entry<Integer, BigDecimal> entry : dtEntry.getValue().entrySet()) {
                // получить одну запись долга по дате
                BigDecimal debForPen = entry.getValue();
                if (debForPen.compareTo(BigDecimal.ZERO) > 0) {
                    Integer mg = entry.getKey();
                    // расчет пени
                    Optional<GenPenMngImpl.PenDTO> penDto = genPenMng.calcPen(calcStore, debForPen, mg, kart, dt);
                    penDto.ifPresent(t -> {
                        // сохранить кол-во дней долга (будет сохранено последнее значение)
                        mapDebDays.put(mg, t.getDays());
                        PenCurRec lastRec = mapLastPenCurRec.get(mg);
                        if (lastRec != null) {
                            if (lastRec.compareWith(mg, debForPen, t.getStavr())) {
                                lastRec.addPenDay(t.getPenya(), dt);
                            } else {
                                PenCurRec rec = new PenCurRec(mg, debForPen, t.getPenya(), t.getStavr(), dt, dt);
                                lstPenCurRec.add(rec);
                                // поменять последнюю запись по пене
                                mapLastPenCurRec.put(mg, rec);
                            }
                        } else {
                            PenCurRec rec = new PenCurRec(mg, debForPen, t.getPenya(), t.getStavr(), dt, dt);
                            lstPenCurRec.add(rec);
                            // поменять последнюю запись по пене
                            mapLastPenCurRec.put(mg, rec);
                        }
                        log.trace("Пеня: debForPen={}, dt={}, mg={}, совокупно дней={}, penya={}, proc={}, Stavr.id={}",
                                debForPen, Utl.getStrFromDate(dt), mg, t.getDays(), t.getPenya(), t.getProc(),
                                t.getStavr() != null ? t.getStavr().getId() : t.getStavr());
                    });
                }
            }
        }
        // сохранить пеню в C_PEN_CUR (C_PEN_CUR нужен для Директа: Пасп.стол->Справка по пене, а так же для распределения
        // текущей пени для оборотки - t_chpenya_for_saldo)
        penCurDAO.deleteByLsk(kart.getLsk());
        // записать в C_PEN_CUR
        lstPenCurRec.forEach(t -> {
            if (t.getPen().compareTo(BigDecimal.ZERO) != 0) {
                log.trace("Пеня по ставкам: mg={}, debPen={}, curDays={}, dt1={}, dt2={}, pen={}, stavr.id={}",
                        t.getMg(), t.getDebForPen(), t.getCurDays(), Utl.getStrFromDate(t.getDt1()),
                        Utl.getStrFromDate(t.getDt2()), t.getPen(), t.getStavr() != null ? t.getStavr().getId() : t.getStavr());
                PenCur penCur = new PenCur();
                penCur.setKart(kart);
                penCur.setMg1(String.valueOf(t.getMg()));
                penCur.setCurDays(t.getCurDays());
                penCur.setStavr(t.getStavr());
                penCur.setDeb(t.getDebForPen());
                penCur.setPenya(t.getPen());
                penCur.setDt1(t.getDt1());
                penCur.setDt2(t.getDt2());
                kart.getPenCur().add(penCur);
                em.persist(penCur);
                log.trace("C_PEN_CUR: период={}, тек.пеня={}, тек.дней={}", t.getMg(), t.getPen(), t.getCurDays());
            }
        });

        // формировать C_PENYA:
        Map<Integer, BigDecimal> mapPenResult = new HashMap<>();
        // вх.сальдо по пене - APENYA
        apenyaDAO.getByLsk(kart.getLsk(), String.valueOf(calcStore.getPeriodBack()))
                .forEach(t -> mapPenResult.put(Integer.parseInt(t.getMg1()), Utl.nvl(t.getPenya(), BigDecimal.ZERO)));
        // прибавить корректировки пени C_PEN_CORR
        penCorrDAO.getByLsk(kart.getLsk())
                .forEach(t -> mapPenResult.merge(Integer.parseInt(t.getDopl()),
                        Utl.nvl(t.getPenya(), BigDecimal.ZERO), BigDecimal::add));
        // вычесть поступление оплаты пени C_KWTP_MG
        kwtpMgDAO.getByLsk(kart.getLsk())
                .forEach(t -> mapPenResult.merge(Integer.parseInt(t.getDopl()),
                        Utl.nvl(t.getPenya(), BigDecimal.ZERO).negate(), BigDecimal::add));
        // добавить текущий период, для отображения свернутой переплаты, если имеется
        mapPenResult.merge(calcStore.getPeriod(), BigDecimal.ZERO, BigDecimal::add);
        // прибавить текущее начисление пени
        lstPenCurRec.forEach(t -> mapPenResult.merge(t.getMg(), t.getPen(), BigDecimal::add));
        // сохранить в C_PENYA
        penDAO.deleteByLsk(kart.getLsk());
        mapPenResult.forEach((k, v) -> {
            if (v != null && v.compareTo(BigDecimal.ZERO) != 0 || mapDeb.get(k) != null
                    && mapDeb.get(k).compareTo(BigDecimal.ZERO) != 0) {
                Penya penya = new Penya();
                penya.setKart(kart);
                penya.setMg1(String.valueOf(k));
                if (v != null) {
                    penya.setPenya(v.setScale(2, RoundingMode.HALF_UP));
                }
                penya.setSumma(Utl.nvl(mapDeb.get(k), BigDecimal.ZERO));
                if (Utl.nvl(mapDeb.get(k), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                    penya.setDays(mapDebDays.get(k));
                } else {
                    penya.setDays(0);
                }
                kart.getPenya().add(penya);
            }
        });
/*
        for (Penya penya : kart.getPenya()) {
            log.info("C_PENYA: mg1={}, penya={}, summa={}, days={}",
                    penya.getMg1(), penya.getPenya(), penya.getSumma(), penya.getDays());
        }
*/
    }

    /**
     * Сгруппировать долг для расчета пени по дате, периоду
     *
     * @param mapDebPart2  - исходная коллекция
     * @param mapDebForPen - результат
     * @param curDt        - дата группировки
     */
    private void groupByDateMg(HashMap<Integer, PeriodSumma> mapDebPart2, Map<Date,
            Map<Integer, BigDecimal>> mapDebForPen, Date curDt) {
        // взять только положительную составляющую, так как для данного периода долга нужно
        // брать только задолженности по услугам, но не переплаты
        mapDebPart2.entrySet().stream()
                .filter(t -> t.getValue().getDebForPen().compareTo(BigDecimal.ZERO) > 0)
                .forEach(t -> {
                    Map<Integer, BigDecimal> mapByDt = mapDebForPen.get(curDt);
                    if (mapByDt != null) {
                        mapByDt.merge(t.getKey(), t.getValue().getDebForPen(), BigDecimal::add);
                    } else {
                        Map<Integer, BigDecimal> map = new HashMap<>();
                        map.put(t.getKey(), t.getValue().getDebForPen());
                        mapDebForPen.put(curDt, map);
                    }
                });
    }

    /**
     * Сгруппировать долг
     *
     * @param mapDebPart2 - исходная коллекция
     * @param mapDeb      - результат
     */
    private void groupByDateMg(HashMap<Integer, PeriodSumma> mapDebPart2,
                               Map<Integer, BigDecimal> mapDeb) {
        // брать и задолженности по услугам, и переплаты
        mapDebPart2.entrySet().stream()
                .filter(t -> t.getValue().getDeb().compareTo(BigDecimal.ZERO) != 0)
                .forEach(t -> mapDeb.merge(t.getKey(), t.getValue().getDeb(), BigDecimal::add));
    }

    /**
     * Перенести переплату
     *
     * @param mapDebPart2 - коллекция для обработки
     */
    private void moveOverpay(HashMap<Integer, PeriodSumma> mapDebPart2) {

        // отсортировать по периоду
        List<Map.Entry<Integer, PeriodSumma>> mapSorted =
                mapDebPart2.entrySet().stream()
                        .sorted(Comparator.comparing(Map.Entry::getKey))
                        .collect(Collectors.toList());

        // перенести переплату
        BigDecimal overPay = BigDecimal.ZERO;
        BigDecimal overPayForPen = BigDecimal.ZERO;
        ListIterator<Map.Entry<Integer, PeriodSumma>> itr = mapSorted.listIterator();
        while (itr.hasNext()) {
            Map.Entry<Integer, PeriodSumma> t = itr.next();

            // долг
            if (itr.hasNext()) {
                // не последний период, перенести переплату, если есть
                if (overPay.add(t.getValue().getDeb()).compareTo(BigDecimal.ZERO) < 0) {
                    overPay = overPay.add(t.getValue().getDeb());
                    t.getValue().setDeb(BigDecimal.ZERO);
                } else {
                    t.getValue().setDeb(overPay.add(t.getValue().getDeb()));
                    overPay = BigDecimal.ZERO;
                }
            } else {
                // последний период
                if (overPay.compareTo(BigDecimal.ZERO) != 0) {
                    t.getValue().setDeb(overPay.add(t.getValue().getDeb()));
                }
            }

            // долг для расчета пени
            if (itr.hasNext()) {
                // не последний период, перенести переплату, если есть
                if (overPayForPen.add(t.getValue().getDebForPen()).compareTo(BigDecimal.ZERO) < 0) {
                    overPayForPen = overPayForPen.add(t.getValue().getDebForPen());
                    t.getValue().setDebForPen(BigDecimal.ZERO);
                } else {
                    t.getValue().setDebForPen(overPayForPen.add(t.getValue().getDebForPen()));
                    overPayForPen = BigDecimal.ZERO;
                }
            } else {
                // последний период
                if (overPayForPen.compareTo(BigDecimal.ZERO) != 0) {
                    t.getValue().setDebForPen(overPayForPen.add(t.getValue().getDebForPen()));
                }
            }

            if (t.getValue().getDeb().compareTo(BigDecimal.ZERO) == 0
                    && t.getValue().getDebForPen().compareTo(BigDecimal.ZERO) == 0) {
                mapDebPart2.remove(t.getKey());
            }
        }
    }

    /**
     * Обработка финансового потока, группировка долгов по услугам, орг, периоду
     *
     * @param stream         - поток
     * @param mapDeb         - результат
     * @param beforeDt       - ограничивать до даты, включительно
     * @param beforeDtForPen - ограничивать до даты, не включая, для пени
     * @param isNegate       - делать отрицательный знак (для оплаты)
     * @param curMg          - текущий период
     */
    private void process(Stream<SumRecMgDt> stream, Map<Integer, PeriodSumma> mapDeb,
                         Date beforeDt, Date beforeDtForPen, boolean isNegate, Integer curMg) {
        stream
                .forEach(t -> {
                            //log.info("Обработка: t.getSumma()={}, t.getDt()={}, t.getMg()={}",
                            //        t.getSumma(), t.getDt(), t.getMg());
                            BigDecimal deb = BigDecimal.ZERO;
                            // ограничить по дате для расчета долга
                            if (beforeDt == null || t.getDt().getTime() <= beforeDt.getTime()) {
                                deb = isNegate ? t.getSumma().negate() : t.getSumma();
                            }
                            BigDecimal debForPen = BigDecimal.ZERO;
                            // ограничить по дате для долга для расчета пени
                            if (beforeDtForPen == null || t.getDt().getTime() < beforeDtForPen.getTime()) {
                                debForPen = isNegate ? t.getSumma().negate() : t.getSumma();
                            }

                            if (deb.compareTo(BigDecimal.ZERO) != 0 || debForPen.compareTo(BigDecimal.ZERO) != 0) {
                                PeriodSumma periodSumma =
                                        new PeriodSumma(deb, debForPen);
                                Integer debPeriod = curMg != null ? curMg : t.getMg();
                                PeriodSumma val = mapDeb.get(debPeriod);
                                if (val == null) {
                                    mapDeb.put(debPeriod, periodSumma);
                                } else {
                                    val.setDeb(val.getDeb().add(periodSumma.getDeb()));
                                    val.setDebForPen(val.getDebForPen().add(periodSumma.getDebForPen()));
                                }
                            }
                        }
                );

    }
}