package com.dic.app.service.impl;

import com.dic.app.RequestConfigDirect;
import com.dic.app.service.ConfigApp;
import com.dic.app.service.GenPart;
import com.dic.bill.dao.NaborDAO;
import com.dic.bill.dto.*;
import com.dic.bill.mm.*;
import com.dic.bill.model.scott.*;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.ErrorWhileChrg;
import com.ric.cmn.excp.WrongParam;
import com.ric.dto.SumMeterVol;
import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.util.Strings;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.*;
import java.util.stream.Collectors;

import static com.dic.app.service.impl.enums.ProcessTypes.CHARGE_SINGLE_USL_4;

@Slf4j
@Service
public class GenPartImpl implements GenPart {

    private final KartMng kartMng;
    private final MeterMng meterMng;
    private final NaborMng naborMng;
    private final KartPrMng kartPrMng;
    private final SprParamMng sprParamMng;
    private final NaborDAO naborDao;
    private final ConfigApp configApp;

    public GenPartImpl(KartMng kartMng, MeterMng meterMng, NaborMng naborMng, KartPrMng kartPrMng,
                       SprParamMng sprParamMng, NaborDAO naborDao, ConfigApp configApp) {
        this.kartMng = kartMng;
        this.meterMng = meterMng;
        this.naborMng = naborMng;
        this.kartPrMng = kartPrMng;
        this.sprParamMng = sprParamMng;
        this.naborDao = naborDao;
        this.configApp = configApp;
    }

    @PersistenceContext
    private EntityManager em;

    /**
     * Расчет объема по услугам
     *
     * @param chrgCountAmountLocal локальное хранилище объемов, по помещению
     * @param reqConf              запрос
     * @param parVarCntKpr         параметр подсчета кол-во проживающих (0-для Кис, 1-Полыс., 1 для ТСЖ (пока, может поправить)
     * @param parCapCalcKprTp      параметр учета проживающих для капремонта
     * @param ko                   объект Ko помещения
     * @param lstMeterVol          объемы по счетчикам
     * @param lstSelUsl            список услуг для расчета
     * @param uslMeterVol       хранилище объемов по счетчикам
     * @param curDt                дата расчета
     * @param part                 группа расчета (услуги рассчитываемые до(1) /после(2) расчета ОДН
     * @throws ErrorWhileChrg ошибка во время расчета
     */
    @Override
    @Transactional(rollbackFor = Exception.class)
    public void genVolPart(ChrgCountAmountLocal chrgCountAmountLocal,
                           RequestConfigDirect reqConf, int parVarCntKpr,
                           int parCapCalcKprTp, Ko ko, List<SumMeterVol> lstMeterVol, List<Usl> lstSelUsl,
                           UslMeterVol uslMeterVol, Date curDt, int part)
            throws ErrorWhileChrg, WrongParam {

        List<Nabor> lstNabor = naborMng.getActualNabor(ko, curDt);
        // объем по услуге, за рассчитанный день
        Map<String, UslPriceVolKart> mapUslPriceVol = new HashMap<>(30);
        // объемы по х.в. г.в. ОДН
        BigDecimal dayColdHotWaterVolODN = BigDecimal.ZERO;
        List<UslMeterDateVol> lstDayMeterVol = uslMeterVol.getLstDayMeterVol();

        for (Nabor nabor : lstNabor) {
            String uslId = nabor.getUsl().getId();
            // получить основной лиц счет по связи klsk помещения
            Kart kartMainByKlsk = em.getReference(Kart.class, kartMng.getKartMainLsk(nabor.getKart()));

            //log.trace("Основной лиц.счет lsk={}", kartMainByKlsk.getLsk());
            if (nabor.getUsl().isMain() && (lstSelUsl.size() == 0 || lstSelUsl.contains(nabor.getUsl()))
                    && (part == 1 && !Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19) ||
                    part == 2 && Utl.in(nabor.getUsl().getFkCalcTp(), 47, 19)) // фильтр очередности расчета
            ) {
                // РАСЧЕТ по основным услугам (из набора услуг или по заданным во вводе)
/*
                log.trace("part={}, {}: lsk={}, uslId={}, fkCalcTp={}, dt={}",
                        part,
                        reqConf.getTpName(),
                        nabor.getKart().getLsk(), nabor.getUsl().getId(),
                        nabor.getUsl().getFkCalcTp(), Utl.getStrFromDate(curDt));
*/
                final Integer fkCalcTp = nabor.getUsl().getFkCalcTp();
                final BigDecimal naborNorm = Utl.nvl(nabor.getNorm(), BigDecimal.ZERO);
                final BigDecimal naborVol = Utl.nvl(nabor.getVol(), BigDecimal.ZERO);
                final BigDecimal naborVolAdd = Utl.nvl(nabor.getVolAdd(), BigDecimal.ZERO);
                // ввод
                final Vvod vvod = nabor.getVvod();
                final boolean isForChrg = nabor.isActive(false);
                // признаки 0 зарег. и наличия счетчика от связанной услуги
                Boolean isLinkedEmpty = null;
                Boolean isLinkedExistMeter = null;

                // тип распределения ввода
                Integer distTp = 0;
                BigDecimal vvodVol = BigDecimal.ZERO;
                boolean isChargeInNotHeatingPeriod = false;
                if (vvod != null) {
                    distTp = vvod.getDistTp();
                    vvodVol = Utl.nvl(vvod.getKub(), BigDecimal.ZERO);
                    isChargeInNotHeatingPeriod = vvod.getIsChargeInNotHeatingPeriod() != null
                            && vvod.getIsChargeInNotHeatingPeriod();
                }
                Kart kartMain;
                if (Utl.in(nabor.getKart().getTp().getCd(), "LSK_TP_ADDIT", "LSK_TP_RSO")) {
                    // дополнит.счета Капрем., РСО
                    // получить родительский лиц.счет, если указан явно
                    kartMain = kartMainByKlsk;
                } else {
                    // основные лиц.счета - взять текущий лиц.счет
                    kartMain = nabor.getKart();
                }

                // наличие счетчика
                boolean isMeterExist = false;
                if (nabor.getUsl().isCounterCalc()) {
                    // услуга с которой получить объем (иногда выполняется перенаправление, например для fkCalcTp=31)
                    final Usl factUslVol = nabor.getUsl().getMeterUslVol() != null ?
                            nabor.getUsl().getMeterUslVol() : nabor.getUsl();
                    // х.в.,г.в узнать, работал ли хоть один счетчик в данном дне
                    isMeterExist = meterMng.isExistAnyMeter(lstMeterVol, factUslVol.getId(), curDt);
                }
                CountPers countPers;
                if (!configApp.getMapParams().get("isDetChrg") && Utl.in(fkCalcTp, 55, 56)) {
                    // ТСЖ расчет до 15 числа и после, по выборочным услугам
                    countPers = getCountPersAmount(parVarCntKpr, parCapCalcKprTp, configApp.getDtMiddleMonth(),
                            nabor, kartMain, isMeterExist);

                    if (!chrgCountAmountLocal.getKprByTp1Added().contains(uslId)) {
                        ChargePrep chargePrep = new ChargePrep();
                        nabor.getKart().getChargePrep().add(chargePrep);
                        chargePrep.setKart(nabor.getKart());
                        chargePrep.setUsl(nabor.getUsl());
                        chargePrep.setKpr(countPers.kpr);
                        chargePrep.setIsExistMeter(isMeterExist);
                        chrgCountAmountLocal.getKprByTp1Added().add(uslId);
                        chargePrep.setTp(1);
                    }
                } else {
                    countPers = getCountPersAmount(parVarCntKpr, parCapCalcKprTp, curDt,
                            nabor, kartMain, isMeterExist);
                }


                SocStandart socStandart = null;
                // наличие счетчика х.в.
                boolean isColdMeterExist = false;
                // наличие счетчика г.в.
                boolean isHotMeterExist = false;
                BigDecimal tempVol = BigDecimal.ZERO;
                // объемы
                BigDecimal dayVol = BigDecimal.ZERO;
                // объем по х.в. для водоотведения
                BigDecimal dayColdWaterVol = BigDecimal.ZERO;
                BigDecimal dayColdWaterVolOverSoc = BigDecimal.ZERO;
                // объем по г.в. для водоотведения
                BigDecimal dayHotWaterVol = BigDecimal.ZERO;
                BigDecimal dayHotWaterVolOverSoc = BigDecimal.ZERO;
                BigDecimal dayVolOverSoc = BigDecimal.ZERO;

                // площади (взять с текущего лиц.счета)
                final BigDecimal kartArea = Utl.nvl(nabor.getKart().getOpl(), BigDecimal.ZERO);
                BigDecimal areaOverSoc = BigDecimal.ZERO;
                int vvodDistTp = naborMng.getVvodDistTp(lstNabor, nabor.getUsl().getParentUsl());
                boolean isMunicipal = nabor.getKart().getStatus().getId().equals(1);

                // получить цены по услуге по лицевому счету из набора услуг!
                final DetailUslPrice detailUslPrice = getDetailUslPrice(uslMeterVol, nabor, kartMain, countPers);

                // расчет
                if (Utl.in(fkCalcTp, 25) // Текущее содержание и подобные услуги (без свыше соц.нормы и без 0 проживающих)
                        || fkCalcTp.equals(7) && isMunicipal // Найм (только по муниципальным помещениям) расчет на м2
                        || (fkCalcTp.equals(24) || fkCalcTp.equals(32) // Прочие услуги, расчитываемые как расценка * норматив * общ.площадь
                        && !isMunicipal)// или 32 услуга, только не по муниципальному фонду
                        || fkCalcTp.equals(36)// Вывоз жидких нечистот и т.п. услуги
                        || fkCalcTp.equals(37) && !isMunicipal // Капремонт в немуницип.фонде
                        || fkCalcTp.equals(50) && vvodDistTp == 4 // если нет счетчика ОДПУ
                        || fkCalcTp.equals(51) && (vvodDistTp == 1 || vvodDistTp == 8) // проп.площади или для информации
                ) {
                    if (fkCalcTp.equals(25)) {
                        // текущее содержание - получить соц.норму
                        socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                        dayVol = kartArea.multiply(reqConf.getPartDayMonth());
                    } else if (fkCalcTp.equals(37)) {
                        // капремонт
                        if (countPers.capPriv != null) {
                            // есть льгота, добавить информацию, если уже не была добавлена в этом периоде
                            if (!chrgCountAmountLocal.isCapPrivAdded()) {
                                ChargePrep chargePrep = new ChargePrep();
                                nabor.getKart().getChargePrep().add(chargePrep);
                                chargePrep.setKart(nabor.getKart());
                                chargePrep.setUsl(nabor.getUsl());
                                chargePrep.setTp(9);
                                chargePrep.setSpk(countPers.capPriv);
                                chargePrep.setIsExistMeter(false);
                                chrgCountAmountLocal.setCapPrivAdded(true);
                            }
                        } else {
                            dayVol = kartArea.multiply(reqConf.getPartDayMonth());
                        }
                    } else {
                        // прочие услуги
                        dayVol = kartArea.multiply(reqConf.getPartDayMonth());
                    }
                } else if (nabor.getUsl().isBaseWaterCalc2()) {
                    // Х.В., Г.В., без уровня соцнормы/свыше, электроэнергия (в т.ч. эл.эн. ТСЖ)
                    // получить объем по нормативу в доле на 1 день
                    if (Utl.in(fkCalcTp, 17, 31, 59) || (Utl.in(fkCalcTp, 18, 52) &&
                            (!Utl.nvl(kartMain.getIsKran1(), false) ||
                                    isMeterExist || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT1"),// кран из системы отопления (не счетчик) -
                                    sprParamMng.getD1("MONTH_HEAT2")) // начислять только в отопительный период
                            ))) {
                        if (reqConf.getTp() == CHARGE_SINGLE_USL_4) {
                            // начисление по выбранной услуге, по нормативу, для автоначисления
                            isMeterExist = false;
                        }

                        // получить соцнорму
                        socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                        if (isMeterExist) {
                            // получить объем по счетчику в пропорции на 1 день его работы
                            UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                    .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                    .findFirst().orElse(null);
                            if (partVolMeter != null) {
                                tempVol = partVolMeter.vol;
                            }
                        } else {
                            // норматив в пропорции на 1 день месяца
                            tempVol = socStandart.vol.multiply(reqConf.getPartDayMonth());
                        }

                        dayVol = tempVol;
                    }
                } else if (Utl.in(fkCalcTp, 53) &&
                        (!Utl.nvl(kartMain.getIsKran1(), false) ||
                                isMeterExist || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT1"),// кран из системы отопления (не счетчик) -
                                sprParamMng.getD1("MONTH_HEAT2")) // начислять только в отопительный период
                        )) {
                    // Компонент на тепл энерг. для Г.В.
                    // получить объем по нормативу в доле на 1 день
                    // получить соцнорму
                    Optional<Usl> uslParentOpt = Optional.ofNullable(configApp.getMapUslByCd().get("COMPHW")); //uslDao.findByCd("COMPHW");
                    if (uslParentOpt.isPresent()) {
                        // получить норматив, кол-во прож, объемы счетчика, от родительской услуги "COMPHW"
                        Optional<Nabor> naborParentOpt = naborDao.findByKartAndUsl(nabor.getKart(),
                                uslParentOpt.get());
                        if (naborParentOpt.isPresent()) {
                            CountPers countPersParent = getCountPersAmount(parVarCntKpr, parCapCalcKprTp, curDt,
                                    naborParentOpt.get(), kartMain, isMeterExist);
                            socStandart = kartPrMng.getSocStdtVol(kartArea, naborParentOpt.get(), countPersParent);
                            if (isMeterExist) {
                                // получить объем по счетчику в пропорции на 1 день его работы
                                UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                        .filter(t -> t.usl.equals(naborParentOpt.get().
                                                getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                        .findFirst().orElse(null);
                                if (partVolMeter != null) {
                                    tempVol = partVolMeter.vol;
                                }
                            } else {
                                // норматив в пропорции на 1 день месяца
                                tempVol = socStandart.vol.multiply(reqConf.getPartDayMonth());
                            }
                            // умножить на норматив гКал * объем м3
                            dayVol = tempVol.multiply(nabor.getNorm());

                        } else {
                            log.error("Для дочерней услуги USL.USL={}, не найдена запись набора с NABOR.LSK={} и NABOR.USL={}",
                                    uslId, nabor.getKart().getLsk(),
                                    uslParentOpt.get().getId());
                        }
                    } else {
                        log.error("Для дочерней услуги USL.USL={}, не найдена услуга по USL.CD=\"COMPHW\"",
                                uslId);
                    }
                } else if (Utl.in(fkCalcTp, 19)) {
                    // Водоотведение без уровня соцнормы/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);

                    List<UslPriceVolKart> lstColdHotWater =
                            chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                    .filter(t -> t.getDt().equals(curDt)
                                            && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                            && t.getUsl().getIsUseVolCan()
                                    ).collect(Collectors.toList());
                    // сложить предварительно рассчитанные объемы х.в.+г.в., найти признаки наличия счетчиков
                    for (UslPriceVolKart t : lstColdHotWater) {
                        if (t.getUsl().getFkCalcTp().equals(17)) {
                            // х.в.
                            dayColdWaterVol = dayColdWaterVol.add(t.getVol());
                            isColdMeterExist = t.isMeter();
                        } else {
                            // г.в.
                            dayHotWaterVol = dayHotWaterVol.add(t.getVol());
                            isHotMeterExist = t.isMeter();
                        }
                    }
                } else if (Utl.in(fkCalcTp, 14)) {
                    // Отопление гкал. без уровня соцнормы/свыше
                    if (Utl.nvl(kartMain.getPot(), BigDecimal.ZERO).compareTo(BigDecimal.ZERO) != 0) {
                        // есть показания по Индивидуальному счетчику отопления
                        tempVol = Utl.nvl(kartMain.getMot(), BigDecimal.ZERO);
                    } else {
                        if (kartArea.compareTo(BigDecimal.ZERO) != 0) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                tempVol = naborVol;
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (isChargeInNotHeatingPeriod) {
                                    // начислять и в НЕотопительном периоде
                                    tempVol = kartArea.multiply(naborNorm);
                                } else {
                                    // начислять только в отопительном периоде
                                    if (Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                            sprParamMng.getD1("MONTH_HEAT4"))) {
                                        tempVol = kartArea.multiply(naborNorm);
                                    }
                                }
                            }
                        }

                    }
                    //  в доле на 1 день
                    // помещение с проживающими
                    dayVol = tempVol.multiply(reqConf.getPartDayMonth());
                } else if (Utl.in(fkCalcTp, 54)) {
                    // Отопление гкал. с уровнем соцнормы/свыше (Полыс.)
                    socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                    if (isMeterExist) {
                        // получить объем по счетчику в пропорции на 1 день его работы
                        UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.equals(curDt))
                                .findFirst().orElse(null);
                        if (partVolMeter != null) {
                            //  в доле на 1 день - по счетчику - весь объем по льготной цене
                            dayVol = partVolMeter.vol;
                        }
                    } else {
                        if (kartArea.compareTo(BigDecimal.ZERO) != 0) {
                            if (distTp.equals(1)) {
                                // есть ОДПУ по отоплению гкал, начислить по распределению
                                tempVol = naborVol.multiply(reqConf.getPartDayMonth());
                            } else if (Utl.in(distTp, 4, 5)) {
                                // нет ОДПУ по отоплению гкал, начислить по нормативу с учётом отопительного сезона
                                if (isChargeInNotHeatingPeriod || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT3"),
                                        sprParamMng.getD1("MONTH_HEAT4"))) {
                                    tempVol = kartArea.multiply(naborNorm).multiply(reqConf.getPartDayMonth());
                                }
                            }
                        }
                        //  в доле на 1 день
                        dayVol = tempVol.multiply(socStandart.procNorm);
                        dayVolOverSoc = tempVol.subtract(dayVol);
                    }

                } else if (Utl.in(fkCalcTp, 58)) {
                    // Отопление м2 с уровнем соцнормы/свыше (ТСЖ)
                    socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
                    tempVol = kartArea.multiply(reqConf.getPartDayMonth());
                    //  в доле на 1 день
                    dayVol = tempVol.multiply(socStandart.procNorm);
                    dayVolOverSoc = tempVol.subtract(dayVol);

                } else if (Utl.in(fkCalcTp, 55, 56)) {
                    // Х.В., Г.В., с соцнормой/свыше (ТСЖ)
                    // получить объем по нормативу в доле на 1 день
                    if (Utl.in(fkCalcTp, 55) || (Utl.in(fkCalcTp, 56) &&
                            (!Utl.nvl(kartMain.getIsKran1(), false) ||
                                    isMeterExist || Utl.between(curDt, sprParamMng.getD1("MONTH_HEAT1"),// кран из системы отопления (не счетчик) -
                                    sprParamMng.getD1("MONTH_HEAT2")) // начислять только в отопительный период
                            ))) {
                        if (reqConf.getTp() == CHARGE_SINGLE_USL_4) {
                            // начисление по выбранной услуге, по нормативу, для автоначисления
                            isMeterExist = false;
                        }

                        if (isMeterExist) {
                            // получить объем по счетчику в пропорции на 1 день его работы
                            UslMeterDateVol partVolMeter = lstDayMeterVol.stream()
                                    .filter(t -> t.usl.equals(nabor.getUsl().getMeterUslVol()) && t.dt.compareTo(curDt) == 0)
                                    .findFirst().orElse(null);
                            if (partVolMeter != null) {
                                tempVol = partVolMeter.vol;
                            }
                            // получить соцнорму
                            socStandart = kartPrMng.getSocStdtVol(
                                    tempVol.multiply(BigDecimal.valueOf(Utl.getCntDaysByDate(curDt))),
                                    nabor, countPers);

                            dayVol = tempVol.multiply(socStandart.procNorm);
                            dayVolOverSoc = tempVol.subtract(dayVol);
                        } else {
                            // норматив в пропорции на 1 день месяца
                            // получить соцнорму
                            socStandart = kartPrMng.getSocStdtVol(BigDecimal.ZERO, nabor, countPers);
                            dayVol = socStandart.vol.multiply(reqConf.getPartDayMonth());
                        }
                    }
                } else if (Utl.in(fkCalcTp, 57)) {
                    // Водоотведение с соцнормой/свыше
                    // получить объем по нормативу в доле на 1 день
                    // узнать, работал ли хоть один счетчик в данном дне

                    List<UslPriceVolKart> lstColdHotWater =
                            chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                    .filter(t -> t.getDt().equals(curDt)
                                            && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                            && t.getUsl().getIsUseVolCan()
                                    ).collect(Collectors.toList());
                    // сложить предварительно рассчитанные объемы х.в.+г.в., найти признаки наличия счетчиков
                    BigDecimal coldWaterVol = BigDecimal.ZERO;
                    BigDecimal hotWaterVol = BigDecimal.ZERO;
                    for (UslPriceVolKart t : lstColdHotWater) {
                        if (t.getUsl().getFkCalcTp().equals(55)) {
                            // х.в.
                            coldWaterVol = coldWaterVol.add(t.getVol().add(t.getVolOverSoc()));
                            isColdMeterExist = t.isMeter();
                        } else if (t.getUsl().getFkCalcTp().equals(56)) {
                            // г.в.
                            hotWaterVol = hotWaterVol.add(t.getVol().add(t.getVolOverSoc()));
                            isHotMeterExist = t.isMeter();
                        }
                    }
                    BigDecimal amountVol = coldWaterVol.add(hotWaterVol);
                    // получить соцнорму
                    socStandart = kartPrMng.getSocStdtVol(
                            amountVol.multiply(BigDecimal.valueOf(Utl.getCntDaysByDate(curDt))), nabor, countPers);
                    BigDecimal partSoc = amountVol.multiply(socStandart.procNorm);
                    BigDecimal partOverSoc = amountVol.subtract(partSoc);
                    BigDecimal ratio = BigDecimal.ONE;
                    if (amountVol.compareTo(BigDecimal.ZERO) != 0) {
                        ratio = coldWaterVol.divide(amountVol, 7, RoundingMode.HALF_UP);
                    }
                    dayColdWaterVol = partSoc.multiply(ratio);
                    dayHotWaterVol = partSoc.subtract(dayColdWaterVol);

                    dayColdWaterVolOverSoc = partOverSoc.multiply(ratio);
                    dayHotWaterVolOverSoc = partOverSoc.subtract(dayColdWaterVolOverSoc);

                } else if (Utl.in(fkCalcTp, 12)) {
                    // Антенна, код.замок
                    dayVol = reqConf.getPartDayMonth();
                } else if (Utl.in(fkCalcTp, 20, 21, 23)) {
                    // Х.В., Г.В., Эл.Эн. содерж.общ.им.МКД, Эл.эн.гараж
                    if (Utl.in(fkCalcTp, 20, 21)) {
                        if (nabor.getUsl().getParentUsl() != null) {
                            // получить наличие счетчика из родительской услуги
                            UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(nabor.getUsl().getParentUsl().getId());
                            isMeterExist = uslPriceVolKart != null && uslPriceVolKart.isMeter();
                        } else {
                            log.error("Пустая главная услуга в поле PARENT_USL, в справочнике услуг, по услуге: usl={}",
                                    uslId);
                        }
                    }
                    dayVol = naborVolAdd.multiply(reqConf.getPartDayMonth());

                    if (fkCalcTp == 20) {
                        // для ОДН Водоотведения
                        dayColdHotWaterVolODN = dayColdHotWaterVolODN.add(dayVol);
                    }
                } else if (Utl.in(fkCalcTp, 34)) {
                    // Повыш.коэфф Полыс
                    if (nabor.getUsl().getParentUsl() != null) {
                        dayVol = reqConf.getPartDayMonth().multiply(naborNorm);
                    } else {
                        throw new ErrorWhileChrg("ОШИБКА! По услуге usl.id=" + uslId +
                                " отсутствует PARENT_USL");
                    }
                } else if (Utl.in(fkCalcTp, 22)) {
                    // ОДН Водоотведения (ТСЖ, от 19.11.2022)
                    // получить объем из Х.в.ОДН, Г.в.ОДН
                    dayVol = dayColdHotWaterVolODN;
                } else if (Utl.in(fkCalcTp, 44, 60)) {
                    // Повыш.коэфф Кис
                    // получить объем из указанных услуг (Например 015+162 объемы услуг)
                    String str = nabor.getUsl().getUslVolList();
                    if (Strings.isNotBlank(str)) {
                        String[] uslList = str.split(",");
                        for (String uslStr : uslList) {
                            boolean isCheckMeter = fkCalcTp == 44;
                            dayVol = dayVol.add(getParentVol(mapUslPriceVol, naborNorm, uslStr, isCheckMeter));
                        }
                    }
                } else if (fkCalcTp.equals(49)) {
                    // Вывоз мусора - кол-во прожив * цену (Кис.)
                    //area = kartArea;
                    dayVol = BigDecimal.valueOf(countPers.kprNorm).multiply(reqConf.getPartDayMonth());
                } else if (fkCalcTp.equals(47)) {
                    // Тепл.энергия для нагрева ХВС (Кис.)
                    //area = kartArea;
                    Optional<Usl> uslParentOpt = Optional.ofNullable(configApp.getMapUslByCd().get("х.в. для гвс")); //uslDao.findByCd("х.в. для гвс");
                    // Usl uslLinked = uslDao.getTaskIdByCd("х.в. для гвс"); убрал 04.02.2021
                    if (uslParentOpt.isPresent()) {
                        Usl uslLinked = uslParentOpt.get();
                        BigDecimal vvodVol2 = ko.getKart().stream()
                                .flatMap(t -> t.getNabor().stream())
                                .filter(t -> t.getUsl().equals(uslLinked))
                                .map(t -> t.getVvod().getKub())
                                .findFirst().orElse(BigDecimal.ZERO);

                        // получить объем по расчетному дню связанной услуги
                        UslPriceVolKart uslPriceVolKart = chrgCountAmountLocal.getLstUslPriceVolKartDetailed().stream()
                                .filter(t -> t.getDt().equals(curDt)
                                        && t.getKart().getKoKw().equals(nabor.getKart().getKoKw())
                                        && t.getUsl().equals(uslLinked))
                                .findFirst().orElse(null);

                        if (uslPriceVolKart != null) {
                            isLinkedEmpty = uslPriceVolKart.isEmpty();
                            isLinkedExistMeter = uslPriceVolKart.isMeter();
                            if (vvodVol2.compareTo(BigDecimal.ZERO) != 0) {
                                dayVol = uslPriceVolKart.getVol().divide(vvodVol2, 20, BigDecimal.ROUND_HALF_UP)
                                        .multiply(vvodVol);
                            }
                        }
                    } else {
                        log.error("Для дочерней услуги USL.USL={}, не найдена услуга по USL.CD=\"х.в. для гвс\"",
                                uslId);
                    }
                } else if (fkCalcTp.equals(6) && countPers.kpr > 0) {
                    // Очистка выгр.ям (Полыс.) (при наличии проживающих)
                    // просто взять цену
                    dayVol = new BigDecimal(countPers.kprNorm).multiply(reqConf.getPartDayMonth());
                }


                // сохранить, сгруппировать расчёт
                UslPriceVolKart uslPriceVolKart;
                if (Utl.in(nabor.getUsl().getFkCalcTp(), 19, 57)) {
                    // водоотведение, добавить составляющие по х.в. и г.в.
                    // было ли учтено кол-во проживающих? для устранения удвоения в стате по водоотведению
                    // ред. 05.04.19 - Кис. попросили делать пустую строку, даже если нет объема, для статы
                    uslPriceVolKart = buildVol(curDt, reqConf, nabor, null, null,
                            kartMain, detailUslPrice, countPers, socStandart, isColdMeterExist,
                            dayColdWaterVol, dayColdWaterVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                    // ред. 05.04.19 - Кис. попросили делать пустую строку, даже если нет объема, для статы
                    // уже были учтены проживающие
                    countPers.kpr = 0;
                    countPers.kprNorm = 0;
                    countPers.kprOt = 0;
                    countPers.kprWr = 0;
                    uslPriceVolKart = buildVol(curDt, reqConf, nabor, null, null,
                            kartMain, detailUslPrice, countPers, socStandart, isHotMeterExist,
                            dayHotWaterVol, dayHotWaterVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                } else {
                    // прочие услуги
                    uslPriceVolKart = buildVol(curDt, reqConf, nabor, isLinkedEmpty, isLinkedExistMeter,
                            kartMain, detailUslPrice, countPers, socStandart, isMeterExist,
                            dayVol, dayVolOverSoc, kartArea, areaOverSoc, isForChrg);
                    // сгруппировать по лиц.счету, услуге, для распределения по вводу
                    chrgCountAmountLocal.groupUslVol(uslPriceVolKart);
                }
                // сохранить расчитанный объем по расчетному дню, (используется для услуги Повыш коэфф.)
                mapUslPriceVol.put(uslId, uslPriceVolKart);

/*
                    if (Utl.in(uslPriceVolKart.getUsl().getId(),"045")) {
                        log.trace("РАСЧЕТ ДНЯ:");
                        log.trace("dt:{} usl={} org={} " +
                                        "empt={} stdt={} " +
                                        "prc={} prcOv={} prcEm={} " +
                                        "vol={} volOv={} ar={} arOv={} " +
                                        "Kpr={} Ot={} Wrz={}",
                                Utl.getStrFromDate(uslPriceVolKart.getDt(), "dd"),
                                uslPriceVolKart.getUsl().getId(), uslPriceVolKart.getOrg().getId(), uslPriceVolKart.isEmpty(),
                                uslPriceVolKart.getSocStdt(), uslPriceVolKart.getPrice(), uslPriceVolKart.getPriceOverSoc(), uslPriceVolKart.getPriceEmpty(),
                                uslPriceVolKart.getVol().setScale(8, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getVolOverSoc().setScale(8, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getArea().setScale(8, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getAreaOverSoc().setScale(8, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getKpr().setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getKprOt().setScale(4, BigDecimal.ROUND_HALF_UP),
                                uslPriceVolKart.getKprWr().setScale(4, BigDecimal.ROUND_HALF_UP));
                    }
*/

            }
        }

    }

    private DetailUslPrice getDetailUslPrice(UslMeterVol uslMeterVol, Nabor nabor, Kart kartMain,
                                             CountPers countPers) throws ErrorWhileChrg {
        final BigDecimal kartArea = Utl.nvl(nabor.getKart().getOpl(), BigDecimal.ZERO);
        SocStandart socStandart = kartPrMng.getSocStdtVol(kartArea, nabor, countPers);
        // todo недоработано! не подсчитан итоговый объем! 07.12.22
        // итоговый объем счетчики+норматив. Пока поставил 0, так как предполагалось под услугу fk_calc_tp=59 (эл.эн.ТСЖ)
        //BigDecimal amountVol = ObjectUtils.firstNonNull(uslMeterVol.getAmountVol().get(nabor.getUsl()), BigDecimal.ZERO)
        //        .add(ObjectUtils.firstNonNull(socStandart.vol, BigDecimal.ZERO));
        BigDecimal amountVol = BigDecimal.ZERO;
        return naborMng.getDetailUslPrice(kartMain, nabor, amountVol);
    }

    /**
     * Получить совокупное кол-во проживающих (родительский и дочерний лиц.счета)
     *
     * @param parVarCntKpr    тип подсчета кол-во проживающих
     * @param parCapCalcKprTp тип подсчета кол-во проживающих для капремонта
     * @param curDt           дата расчета
     * @param nabor           строка услуги
     * @param kartMain        основной лиц.счет
     * @param isMeterExist    наличие счетчика в расчетный день (работает только в КИС)
     */
    private CountPers getCountPersAmount(int parVarCntKpr, int parCapCalcKprTp,
                                         Date curDt, Nabor nabor, Kart kartMain, boolean isMeterExist) {
        CountPers countPers;
        countPers = kartPrMng.getCountPersByDate(kartMain, nabor,
                parVarCntKpr, parCapCalcKprTp, curDt, isMeterExist);

        if (nabor.getKart().getParentKart() != null) {
            // в дочернем лиц.счете (привязка)
            // для определения расценки по родительскому (если указан по parentKart) или текущему лиц.счету
            CountPers countPersParent = kartPrMng.getCountPersByDate(nabor.getKart().getParentKart(), nabor,
                    parVarCntKpr, parCapCalcKprTp, curDt, isMeterExist);
            countPers.kpr = countPersParent.kpr;
            countPers.isEmpty = countPersParent.isEmpty;

            if (parVarCntKpr == 0
                    && countPers.kprNorm == 0 &&
                    !kartMain.getStatus().getCd().equals("MUN")) {
                // вариант Кис.
                if (countPers.kprOt == 0) {
                    // в РСО счетах и кол-во временно отсут.=0
                    countPers.kprNorm = 1;
                } else {
                    if (nabor.getUsl().isCounterCalcExt()) {
                        if (isMeterExist) {
                            // х.в. г.в. водоотв., есть только ВО и только если счетчики
                            // (по сути здесь kprNorm ни на что не влияет, но решил заполнять)
                            // - КИС согласно ТЗ на 22.10.2019
                            countPers.kprNorm = countPers.kprOt;
                        }
                    }
                }
            }

        } else {
            // в родительском лиц.счете (может быть Основным, РСО и прочее, НЕ привязка)
            if (countPers.kprNorm == 0) {
                if (parVarCntKpr == 0) {
                    // Киселёвск
                    // ред. 24.05.2019
                    /* Алёна: Сделать начисление на 1 собственника по услугам 140, 016,012,014,
                        если квартира 0 и муниц.(у нас на 0 квартиры, на приват. начисляется сейчас).
                       Например адрес Багратиона 42-500, муниц.кв. с 0 прожив.
                       Начисление по данным услугам не ведется.*/
/*                    if (!kartMain.getStatus().getCd().equals("MUN")) {
                        // не муницип. помещение
                        if (nabor.getUsl().getFkCalcTp().equals(49)) {
                            // услуга по обращению с ТКО
                            countPers.kpr = 1;
                            countPers.kprNorm = 1;
                        } else if (countPers.kprOt == 0) {
                            countPers.kprNorm = 1;
                        } */

                    if (nabor.getUsl().isCounterCalcExt()) {
                        if (isMeterExist && countPers.kprOt >= 0) {
                            // х.в. г.в. водоотв., есть только ВО и только если счетчики
                            // (по сути здесь kprNorm ни на что не влияет, но решил заполнять)
                            // - КИС согласно ТЗ на 22.10.2019
                            countPers.kprNorm = countPers.kprOt;
                        } else if (countPers.kprOt == 0) {
                            countPers.kprNorm = 1;
                        }
                    } else if (nabor.getUsl().getFkCalcTp().equals(49)) {
                        // услуга по обращению с ТКО
                        countPers.kpr = 1;
                        countPers.kprNorm = 1;
                    } else if (countPers.kprOt == 0) {
                        // прочие услуги
                        countPers.kprNorm = 1;
                    }

                } else if (parVarCntKpr == 1 && countPers.kprOt == 0) {
                    // Полысаево
                    countPers.kprNorm = 1;
                } else if (parVarCntKpr == 2) {
                    // ТСЖ
                    if (nabor.getUsl().getFkCalcTp().equals(49)) {
                        // услуга по обращению с ТКО
                        countPers.kpr = Utl.nvl(kartMain.getKprOwn(), 0);
                        countPers.kprNorm = Utl.nvl(kartMain.getKprOwn(), 0);
                    }
                }
            }
        }
        countPers.isEmpty = countPers.kpr == 0;
        if (nabor.getUsl().getFkCalcTp().equals(34)) {
            // по Повыш коэфф. для Полыс - не учитывается пустое-не пустое помещение
            countPers.isEmpty = false;
        }
        return countPers;
    }

    private BigDecimal getParentVol(Map<String, UslPriceVolKart> mapUslPriceVol, BigDecimal naborNorm, String uslId, boolean isCheckMeter) {
        BigDecimal dayVol = BigDecimal.ZERO;
        UslPriceVolKart uslPriceVolKart = mapUslPriceVol.get(uslId);
        if (uslPriceVolKart != null && (!isCheckMeter || !uslPriceVolKart.isMeter())) {
            // только если нет счетчика в родительской услуге - ред.01.03.23 кис.решили включать счетчики
            // сложить все объемы родит.услуги, умножить на норматив текущей услуги
            dayVol = (uslPriceVolKart.getVol().add(uslPriceVolKart.getVolOverSoc()))
                    .multiply(naborNorm);
        }
        return dayVol;
    }

    /**
     * Построить объем для начисления
     *
     * @param curDt          дата расчета
     * @param reqConf        запрос
     * @param nabor          строка набора
     * @param kartMain       лиц.счет
     * @param detailUslPrice инф. о расценке
     * @param countPers      инф. о кол.прожив.
     * @param socStandart    соцнорма
     * @param isMeterExist   наличие счетчика
     * @param dayVol         объем
     * @param dayVolOverSoc  объем свыше соц.нормы
     * @param kartArea       площадь
     * @param areaOverSoc    площадь свыше соц.нормы
     * @param isForChrg      сохранять в начислении? (C_CHARGE)
     */
    private UslPriceVolKart buildVol(Date curDt, RequestConfigDirect reqConf, Nabor nabor, Boolean isLinkedEmpty,
                                     Boolean isLinkedExistMeter, Kart kartMain, DetailUslPrice detailUslPrice,
                                     CountPers countPers, SocStandart socStandart, boolean isMeterExist,
                                     BigDecimal dayVol, BigDecimal dayVolOverSoc, BigDecimal kartArea,
                                     BigDecimal areaOverSoc, boolean isForChrg) {
        return UslPriceVolKart.UslPriceVolBuilder.anUslPriceVol()
                .withDt(curDt)
                .withKart(nabor.getKart()) // группировать по лиц.счету из nabor!
                .withUsl(nabor.getUsl())
                .withUslOverSoc(detailUslPrice.uslOverSoc)
                .withUslEmpt(detailUslPrice.uslEmpt)
                .withOrg(nabor.getOrg())
                .withIsMeter(isLinkedExistMeter != null ? isLinkedExistMeter : isMeterExist)
                .withIsEmpty(isLinkedEmpty != null ? isLinkedEmpty : countPers.isEmpty)
                .withIsResidental(kartMain.isResidental())
                .withSocStdt(socStandart != null ? socStandart.norm : BigDecimal.ZERO)
                .withPrice(detailUslPrice.price)
                .withPriceOverSoc(detailUslPrice.priceOverSoc)
                .withPriceEmpty(detailUslPrice.priceEmpt)
                .withVol(dayVol)
                .withVolOverSoc(dayVolOverSoc)
                .withArea(kartArea)
                .withAreaOverSoc(areaOverSoc)
                .withKpr(countPers.kpr)
                .withKprOt(countPers.kprOt)
                .withKprWr(countPers.kprWr)
                .withKprNorm(countPers.kprNorm)
                .withPartDayMonth(reqConf.getPartDayMonth())
                .withIsForChrg(isForChrg)
                .build();
    }


}
