package com.dic.app.service.impl;

import com.dic.app.service.ConfigApp;
import com.dic.bill.Lock;
import com.dic.bill.dao.*;
import com.dic.bill.dto.SprPenKey;
import com.dic.bill.mm.ParMng;
import com.dic.bill.mm.SprParamMng;
import com.dic.bill.mm.impl.SprParamMngImpl;
import com.dic.bill.model.scott.*;
import com.dic.bill.model.sec.User;
import com.ric.cmn.Utl;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.persistence.EntityManager;
import java.io.File;
import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

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
@RequiredArgsConstructor
public class ConfigAppImpl implements ConfigApp {

    private final ApplicationContext ctx;
    private final EntityManager em;
    private final SprParamMng sprParamMng;
    private final ParMng parMng;
    private final UslDAO uslDAO;
    private final UslRoundDAO uslRoundDAO;
    private final TuserDAO tuserDAO;
    private final UserDAO userDAO;
    private final OrgDAO orgDAO;

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
    private Map<SprPenKey, SprPen> mapSprPen = new HashMap<>();
    // справочник дат
    private Map<String, Date> mapDate = new HashMap<>();
    // справочник булевых параметров
    private Map<String, Boolean> mapParams = new HashMap<>();
    // справочники кодов услуг для перерасчетов
    private Set<String> waterUslCodes;
    private Set<String> wasteUslCodes;
    private Set<String> waterOdnUslCodes;
    private Set<String> wasteOdnUslCodes;
    // справочник услуг, по CD
    private Map<String, Usl> mapUslByCd;
    // справочник кодов услуг, для округления начисления, для ГИС ЖКХ
    private Map<String, Set<String>> mapUslRound = new HashMap<>();
    private Map<String, Org> mapReuOrg = new HashMap<>();
    // соответствие класса entity - типу поля Id, для evict cache
    private Map<String, Object> mapClassId = new HashMap<>();

    @Value("${gisVersion}")
    private String gisVersion;
    @Value("${hostIp}")
    private String hostIp;
    // запускать ли потоки ГИС на старте
    private boolean isGisWorkOnStart;


    @PostConstruct
    private void setUp() {
        reloadSprPen();
        reloadParam();
        loadUslCodes();
        loadIdEntitiesForCache();
        // блокировщик процессов
        setLock(new Lock());

        setUpGisParameters();
    }


    private void setUpGisParameters() {
        File tempFile = new File("stopGis");
        boolean exists = tempFile.exists();
        if (exists) {
            isGisWorkOnStart = false;
        } else {
            isGisWorkOnStart = true;
        }
    }

    @Override
    public String getPeriod() {
        return Utl.getPeriodFromDate(mapDate.get("dtFirst"));
    }

    @Override
    @Transactional(propagation = Propagation.REQUIRED)
    public Tuser getCurUser() {
        return tuserDAO.findByCd("GEN");
    }

    @Transactional(propagation = Propagation.REQUIRED)
    @Override
    public Optional<User> getCurUserGis() {
        return userDAO.findById(1);
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

    @Override
    public String getPeriodBackByMonth(int month) {
        try {
            return Utl.addMonths(Utl.getPeriodFromDate(mapDate.get("dtFirst")), month);
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

    /**
     * Загрузка кодов услуг для перерасчетов
     */
    private void loadUslCodes() {
        mapUslByCd = uslDAO.findAll().stream().filter(t -> t.getCd() != null).collect(Collectors.toMap(Usl::getCd, t -> t));

        waterUslCodes = uslDAO.findByCdIn(Arrays.asList("х.вода", "х.вода 0 рег.", "х.вода/св.нор", "г.вода", "г.вода/св.нор", "г.вода, 0 рег.", "COMPHW", "COMPHW2"))
                .stream().map(Usl::getId).collect(Collectors.toUnmodifiableSet());
        wasteUslCodes = uslDAO.findByCdIn(Arrays.asList("канализ", "канализ/св.нор", "канализ 0 рег."))
                .stream().map(Usl::getId).collect(Collectors.toUnmodifiableSet());

        waterOdnUslCodes = uslDAO.findByCdIn(Arrays.asList("х.вода.ОДН", "г.вода.ОДН"))
                .stream().map(Usl::getId).collect(Collectors.toUnmodifiableSet());
        wasteOdnUslCodes = uslDAO.findByCdIn(List.of("канализ.ОДН"))
                .stream().map(Usl::getId).collect(Collectors.toUnmodifiableSet());
        mapUslRound =
                uslRoundDAO.findAll().stream().collect(Collectors.groupingBy(UslRound::getReu,
                        Collectors.mapping(t -> t.getUsl().getId(), Collectors.toSet())));
        mapReuOrg = orgDAO.getAllUk().stream().collect(Collectors.toMap(Org::getReu, t -> t));

    }

    /**
     * Получить все Поля Id Entities, для использования при evict в Hibernate L2C cache
     */
    private void loadIdEntitiesForCache() {
        Set<Class<?>> classes = getAllClassesForCaching();
        log.info("Найдено {} классов для кэширования по Id", classes.size());
        if (classes.size() == 0) {
            throw new RuntimeException("Не найдены классы для кэширования по Id");
        }
        for (Class<?> aClass : classes) {
            Class<?> foundClass;
            try {
                boolean found = findAnnotationByName(aClass, "javax.persistence.Table");
                if (!found)
                    continue;
                found = findAnnotationByName(aClass, "javax.persistence.Cacheable");
                if (!found)
                    continue;
                foundClass = Class.forName(aClass.getName());
            } catch (ClassNotFoundException e) {
                log.error("Не найден класс по имени {}", aClass.getName());
                throw new RuntimeException("Не найден класс по имени " + aClass.getName());
            }
            Field[] fields = foundClass.getDeclaredFields();
            // ищем поле с аннотацией @Id в данном классе, заполняем mapClassId
            findAndSetFieldByAnnotation(aClass, fields, "javax.persistence.Id");
        }
    }

    private Set<Class<?>> getAllClassesForCaching() {
        String classes = """
                 com.dic.bill.model.scott.Acharge
                 com.dic.bill.model.scott.AchargePrep
                 com.dic.bill.model.scott.Akart
                 com.dic.bill.model.scott.AkartPr
                 com.dic.bill.model.scott.Akwtp
                 com.dic.bill.model.scott.AkwtpMg
                 com.dic.bill.model.scott.Anabor
                 com.dic.bill.model.scott.Apenya
                 com.dic.bill.model.scott.BaseNabor
                 com.dic.bill.model.scott.Change
                 com.dic.bill.model.scott.ChangeDoc
                 com.dic.bill.model.scott.Charge
                 com.dic.bill.model.scott.ChargePay
                 com.dic.bill.model.scott.ChargePayId
                 com.dic.bill.model.scott.ChargePrep
                 com.dic.bill.model.scott.ChargePrepId
                 com.dic.bill.model.scott.Comps
                 com.dic.bill.model.scott.CorrectPay
                 com.dic.bill.model.scott.Deb
                 com.dic.bill.model.scott.Doc
                 com.dic.bill.model.scott.House
                 com.dic.bill.model.scott.Kart
                 com.dic.bill.model.scott.KartDetail
                 com.dic.bill.model.scott.KartExt
                 com.dic.bill.model.scott.KartPr
                 com.dic.bill.model.scott.Ko
                 com.dic.bill.model.scott.Kwtp
                 com.dic.bill.model.scott.KwtpDay
                 com.dic.bill.model.scott.KwtpDayLog
                 com.dic.bill.model.scott.KwtpMg
                 com.dic.bill.model.scott.KwtpPay
                 com.dic.bill.model.scott.LoadBank
                 com.dic.bill.model.scott.LoadKartExt
                 com.dic.bill.model.scott.Lst
                 com.dic.bill.model.scott.LstTp
                 com.dic.bill.model.scott.Meter
                 com.dic.bill.model.scott.Nabor
                 com.dic.bill.model.scott.Nabors
                 com.dic.bill.model.scott.ObjPar
                 com.dic.bill.model.scott.Org
                 com.dic.bill.model.scott.OrgTp
                 com.dic.bill.model.scott.Param
                 com.dic.bill.model.scott.Pen
                 com.dic.bill.model.scott.PenCorr
                 com.dic.bill.model.scott.PenCur
                 com.dic.bill.model.scott.PenDt
                 com.dic.bill.model.scott.PenRef
                 com.dic.bill.model.scott.PenUslCorr
                 com.dic.bill.model.scott.Penya
                 com.dic.bill.model.scott.PrepErr
                 com.dic.bill.model.scott.Price
                 com.dic.bill.model.scott.RedirPay
                 com.dic.bill.model.scott.Relation
                 com.dic.bill.model.scott.SaldoUsl
                 com.dic.bill.model.scott.SessionDirect
                 com.dic.bill.model.scott.Spk
                 com.dic.bill.model.scott.SpkGr
                 com.dic.bill.model.scott.SprGenItm
                 com.dic.bill.model.scott.SprParam
                 com.dic.bill.model.scott.SprPen
                 com.dic.bill.model.scott.SprProcPay
                 com.dic.bill.model.scott.Spul
                 com.dic.bill.model.scott.StatePr
                 com.dic.bill.model.scott.StateSch
                 com.dic.bill.model.scott.Status
                 com.dic.bill.model.scott.StatusPr
                 com.dic.bill.model.scott.Stavr
                 com.dic.bill.model.scott.Stub
                 com.dic.bill.model.scott.TempObj
                 com.dic.bill.model.scott.Tuser
                 com.dic.bill.model.scott.UserPerm
                 com.dic.bill.model.scott.Usl
                 com.dic.bill.model.scott.UslRound
                 com.dic.bill.model.scott.UslRoundId
                 com.dic.bill.model.scott.VchangeDet
                 com.dic.bill.model.scott.VchangeDetId
                 com.dic.bill.model.scott.Vvod
                 com.dic.bill.model.scott.Xitog3Lsk
                 
                 com.dic.bill.model.exs.DebSubRequest
                 com.dic.bill.model.exs.Eolink
                 com.dic.bill.model.exs.EolinkPar
                 com.dic.bill.model.exs.EolinkToEolink
                 com.dic.bill.model.exs.MeterVal
                 com.dic.bill.model.exs.Notif    
                 com.dic.bill.model.exs.Pdoc     
                 com.dic.bill.model.exs.TaskPar  
                 com.dic.bill.model.exs.TaskToTask
                 com.dic.bill.model.exs.Ulist    
                 com.dic.bill.model.exs.UlistTp  
                 
                 com.dic.bill.model.bs.AddrTp
                 com.dic.bill.model.bs.Lst2
                 com.dic.bill.model.bs.LstTp2
                 com.dic.bill.model.bs.Par
                 
                 com.dic.bill.model.sec.User
                """;

        String[] str = classes.split("\n");

        Set<Class<?>> set = new HashSet<>();
        try {
            for (String s : str) {
                if (s.trim().length() > 0) {
                    set.add(Class.forName(s.trim()));
                }
            }
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }
        return set;
    }

    private boolean findAnnotationByName(Class<?> aClass, String annotationName) {
        Annotation[] annotations = aClass.getAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation.annotationType().getName().equals(annotationName)) {
                return true;
            }
        }
        return false;
    }

    private void findAndSetFieldByAnnotation(Class<?> aClass, Field[] fields, String annotationName) {
        boolean found = false;
        for (Field fld : fields) {
            Annotation[] annotations = fld.getDeclaredAnnotations();
            for (Annotation annotation : annotations) {
                if (annotation.annotationType().getName().equals(annotationName)) {
                    mapClassId.put(aClass.getName(), fld.getType());
                    log.info("В кэш Id entities загружен class={}, Id type={}", aClass.getName(), fld.getType().getName());
                    found = true;
                    break;
                }
            }
            if (found)
                break;
        }
        if (!found) {
            String err = String.format("В классе %s не найдено поле с аннотацией @Id", aClass.getName());
            log.error(err);
            throw new RuntimeException(err);
        }
    }

}
