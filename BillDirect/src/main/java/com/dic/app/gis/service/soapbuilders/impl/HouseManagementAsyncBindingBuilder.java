package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.*;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.maintaners.impl.UlistMng;
import com.dic.app.gis.service.soap.SoapConfigs;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.service.ConfigApp;
import com.dic.bill.UlistDAO;
import com.dic.bill.dao.*;
import com.dic.bill.dto.HouseUkTaskRec;
import com.dic.bill.mm.KartMng;
import com.dic.bill.mm.LstMng;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.bs.AddrTp;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.Ulist;
import com.dic.bill.model.scott.*;
import com.diffplug.common.base.Errors;
import com.ric.cmn.CommonErrs;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType.Error;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.base.OKTMORefType;
import ru.gosuslugi.dom.schema.integration.house_management.*;
import ru.gosuslugi.dom.schema.integration.house_management.ApartmentHouseUOType.BasicCharacteristicts;
import ru.gosuslugi.dom.schema.integration.house_management.ExportHouseResultType.ApartmentHouse.Entrance;
import ru.gosuslugi.dom.schema.integration.house_management.ExportHouseResultType.ApartmentHouse.NonResidentialPremises;
import ru.gosuslugi.dom.schema.integration.house_management.ImportHouseUORequest.ApartmentHouse;
import ru.gosuslugi.dom.schema.integration.house_management.ImportHouseUORequest.ApartmentHouse.*;
import ru.gosuslugi.dom.schema.integration.house_management.ImportHouseUORequest.ApartmentHouse.ResidentialPremises.ResidentialPremisesToCreate;
import ru.gosuslugi.dom.schema.integration.house_management.ImportHouseUORequest.ApartmentHouse.ResidentialPremises.ResidentialPremisesToUpdate;
import ru.gosuslugi.dom.schema.integration.house_management_service_async.HouseManagementPortsTypeAsync;
import ru.gosuslugi.dom.schema.integration.house_management_service_async.HouseManagementServiceAsync;
import ru.gosuslugi.dom.schema.integration.individual_registry_base.ID;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiRef;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;
import java.math.BigDecimal;
import java.text.ParseException;
import java.time.LocalDate;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Сервис обмена информацией с ГИС ЖКХ по Дому
 */
@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class HouseManagementAsyncBindingBuilder {

    private final ApplicationContext ctx;
    @PersistenceContext
    private EntityManager em;
    private final UlistMng ulistMng;
    private final ConfigApp config;
    private final TaskParMng taskParMng;
    private final EolinkDAO eolinkDao;
    private final EolinkDAO2 eolinkDao2;
    private final AddrTpDAO addrTpDAO;
    private final OrgDAO orgDAO;
    private final UlistDAO ulistDAO;
    private final EolinkParMng eolinkParMng;
    private final TaskEolinkParMng teParMng;
    private final TaskDAO taskDao;
    private final KartMng kartMng;
    private final EolinkMng eolinkMng;
    private final TaskMng taskMng;
    private final LstMng lstMng;
    private final SoapConfigs soapConfig;
    private final MeterMng meterMng;
    private final MeterDAO meterDAO;
    private final PseudoTaskBuilder ptb;
    private final EolinkParMng eolParMng;
    @Value("${parameters.gis.meter.usl.check.energ}")
    private String energyUsl;
    @Value("${parameters.gis.meter.exp.par}")
    private Boolean isMeterExpPar; // выгружать параметры счетчиков из ГИС?

    private final class PremiseWithMeter {
        String premiseGUID;
        int meterTp = 0;
        boolean skip = false;
    }

    @Getter
    @Setter
    static class LskEolParam {
        String accountGUID;
        String accountNumber;
        String unifiedAccountNumber;
        String serviceID;
        Eolink eolink;
        Boolean isClosed;
    }

    @AllArgsConstructor
    static class SoapPar {
        private HouseManagementPortsTypeAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    /**
     * Инициализация - создать сервис и порт
     */
    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        HouseManagementServiceAsync service = new HouseManagementServiceAsync();
        HouseManagementPortsTypeAsync port = service.getHouseManagementPortAsync();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);
        sb.setUp((BindingProvider) port, true, reqProp.getPpGuid(), reqProp.getHostIp(), true);

        // логгинг запросов
        sb.setTrace(task.getTrace().equals(1));
        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }


    /**
     * Получить состояние запроса
     *
     * @param task - задание
     */
    private GetStateResult getState2(Task task) throws CantPrepSoap, CantSendSoap {
        // Признак ошибки
        boolean err = false;
        // Признак ошибки в CommonResult
        String errStr = null;
        ru.gosuslugi.dom.schema.integration.house_management.GetStateResult state = null;

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!

        String errMsg = null;
        try {
            state = par.port.getState(gs);
        } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e) {
            errMsg = e.getFaultInfo().getErrorCode();
            e.printStackTrace();
            err = true;
            errStr = "Запрос вернул ошибку!";
        }

        if (state != null && state.getRequestState() != 3) {
            // вернуться, если задание всё еще не выполнено
            log.info("Статус запроса={}, Task.id={}", state.getRequestState(), task.getId());

            if (state.getRequestState() == 1) {
                // статус запроса - ACK - увеличить время ожидания
                taskMng.alterDtNextStart(task);
            }
            return null;
        }

        // Показать ошибки, если есть
        if (err) {
            // Ошибки во время выполнения
            log.info(errStr);
            task.setState("ERR");
            task.setResult(errStr);
            task.setResult(errMsg);
            log.error("Task.id={}, ОШИБКА выполнения запроса = {}", task.getId(), errStr);
        } else {
            if (state == null) {
                log.error("ОШИБКА! state==null");
            } else if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null) {
                // Ошибки контролей или бизнес-процесса
                errStr = state.getErrorMessage().getDescription();
                log.info("Ошибка выполнения запроса = {}", errStr);
                task.setState("ERR");
                task.setResult(errStr);
            }
        }

        return state;

    }


    /**
     * Экспортировать данные счетчиков
     */

    public void exportDeviceData(Integer taskId) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);

        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ExportMeteringDeviceDataRequest req = new ExportMeteringDeviceDataRequest();

        req.setId("foo");
        //sb.setSign(true);
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        if (task.getGuid() != null) {
            // По GUID еще не созданного счетчика
            req.setMeteringDeviceRootGUID(task.getGuid());
        } else if (task.getEolink() != null) {
            // По дому
            req.setFIASHouseGuid(task.getEolink().getGuid());
        } else {
            throw new CantPrepSoap("Не указан один из объектов для выгрузки счетчиков!");
        }

        // искать архивные - ред.12.08.20 сделал принудительно выгрузку архивных,
        // так как не помечались архивными в exs.eolink счетчики
        //Calendar cal = Calendar.getInstance();
        //cal.add(Calendar.YEAR, 1); // искать архивные счетчики, в т.ч. созданные 1 год назад
        //Date prevYear = cal.getTime();
        req.setSearchArchived(false);
        //req.setArchiveDateFrom(getXMLGregorianCalendarFromDate(prevYear));
        //req.setArchiveDateTo(getXMLGregorianCalendarFromDate(new Date()));

        try {
            ack = par.port.exportMeteringDeviceData(req);
        } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e1) {
            e1.printStackTrace();
            err = true;
            errMainStr = e1.getFaultInfo().getErrorMessage();
        }

        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
            taskMng.logTask(task, false, false);
        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
            taskMng.logTask(task, false, true);
        }

    }


    /**
     * Получить результат экспорта счетчиков
     *
     * @throws ErrorProcessAnswer - ошибка процессинга ответа
     * @throws WrongGetMethod     - ошибка получения параметра
     */

    @CacheEvict(value = {"EolinkDAOImpl.getEolinkByGuid"}, allEntries = true) // здесь Evict потому что
    // пользователь может обновить Ko объекта счетчика мз Директа(осуществить привязку)
    // и тогда должен быть получен обновленный объект! ред.07.12.18
    public void exportDeviceDataAsk(Integer taskId) throws ErrorProcessAnswer, WrongGetMethod, UnusableCode, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        setUp(task);

        Eolink houseEol = task.getEolink();

        // получить состояние
        GetStateResult retState = getState2(task);
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {

            // автоматическое связывание счетчика с SCOTT.METER в Директ
            Boolean autoBind = taskParMng.getBool(task, "ГИС ЖКХ.AUTO_CONNECT_DIRECT");
            log.trace("autoBind={}", autoBind);
            // Ошибок не найдено
            for (ExportMeteringDeviceDataResultType meterType : retState.getExportMeteringDeviceDataResult()) {
                // тип счетчика: 0 - жилой ИПУ, 1 - не жилой ИПУ, 2 - общедомовой ПУ
                log.trace("Получен счетчик:");
                log.trace("Root GUID={}", meterType.getMeteringDeviceRootGUID());
                log.trace("Version GUID={}", meterType.getMeteringDeviceVersionGUID());
                log.trace("GISGKHNumber={}", meterType.getMeteringDeviceGISGKHNumber());
                MeteringDeviceBasicCharacteristicsType basicChar = meterType.getBasicChatacteristicts();
                log.trace("Серийный номер={}", basicChar.getMeteringDeviceNumber());

                PremiseWithMeter premiseWithMeter = getPremiseWithMeter(basicChar, houseEol, meterType);
                if (premiseWithMeter.skip) continue;
                // найти корневую запись счетчика
                Eolink rootEol = eolinkDao2.findEolinkByGuid(meterType.getMeteringDeviceRootGUID());
                // найти версию счетчика, по GUID
                Eolink versionEol = eolinkDao2.findEolinkByGuid(meterType.getMeteringDeviceVersionGUID());
                // найти помещение, к которому прикреплен счетчик
                Eolink premiseEol = eolinkDao2.findEolinkByGuid(premiseWithMeter.premiseGUID);

                if (rootEol == null) {
                    // не найдено, создать новую корневую запись счетчика
                    AddrTp addrTp = lstMng.getAddrTpByCD("СчетчикФизический");

                    if (premiseWithMeter.meterTp == 0 || premiseWithMeter.meterTp == 1) {
                        // счетчик жилых или нежилых помещений
                        rootEol = Eolink.builder().withGuid(meterType.getMeteringDeviceRootGUID()).withUn(meterType.getMeteringDeviceGISGKHNumber()).withObjTp(addrTp).withParent(premiseEol).withUser(config.getCurUserGis().get()).withStatus(1).build();
                    } else {
                        // счетчик общедомовой
                        rootEol = Eolink.builder().withGuid(meterType.getMeteringDeviceRootGUID()).withUn(meterType.getMeteringDeviceGISGKHNumber()).withObjTp(addrTp).withParent(premiseEol).withUser(config.getCurUserGis().get()).withStatus(1).build();
                    }

                    log.trace("Попытка создать запись корневого счетчика в Eolink: GUID={}", meterType.getMeteringDeviceRootGUID());
                    em.persist(rootEol);

                }

                // обновить параметры созданного счетчика или уже имеющегося
                if (Utl.nvl(rootEol.getStatus(), 0) == 1 && meterType.getStatusRootDoc().equals("Archival")) {
                    // счетчик активный, отметить архивным
                    rootEol.setStatus(0);
                    log.trace("Попытка отметить счетчик АРХИВНЫМ");
                } else if (Utl.nvl(rootEol.getStatus(), 0) != 1 && meterType.getStatusRootDoc().equals("Active")) {
                    // счетчик архивный, отметить активным
                    rootEol.setStatus(1);
                    log.trace("Попытка отметить счетчик АКТИВНЫМ");
                }

                log.trace("isConsumedVolume={}", basicChar.isConsumedVolume());

                String usl = null;
                // счетчик предоставляет ОБЪЕМ
                for (DeviceMunicipalResourceType d : meterType.getMunicipalResources()) {
                    try {
                        usl = ulistMng.getUslByResource(d.getMunicipalResource());
                    } catch (WrongParam wrongParam) {
                        log.error("ОШИБКА во время получения услуги из справочника");
                        throw new ErrorProcessAnswer("ОШИБКА во время получения услуги из справочника");
                    }
                    //servCd = ulistMng.getServCdByResource(d.getMunicipalResource());
                    break; // XXX Lev: Сделал выход, по первому элементу, пока так, в будущем
                    // надо будет сделать возможность наличия несколько услуг для одного счетчика!
                }

                if (usl == null) {
                    // счетчик предоставляет ПОКАЗАНИЯ
                    List<MunicipalResourceNotElectricExportType> munResNenerg = meterType.getMunicipalResourceNotEnergy();
                    MunicipalResourceElectricExportType munResEl = meterType.getMunicipalResourceEnergy();
                    // проверить, заполнить usl
                    if (munResNenerg.size() > 0) {
                        // Коммунальные услуги, получить первый попавшийся код усл
                        // может в Отоплении будут другие коды услуг!
                        for (MunicipalResourceNotElectricExportType m : munResNenerg) {
                            //log.trace("res.GUID={}", m.getMunicipalResource().getGUID());
                            try {
                                usl = ulistMng.getUslByResource(m.getMunicipalResource());
                            } catch (WrongParam wrongParam) {
                                log.error("ОШИБКА во время получения услуги из справочника");
                                throw new ErrorProcessAnswer("ОШИБКА во время получения услуги из справочника");
                            }
                            //servCd = ulistMng.getServCdByResource(m.getMunicipalResource());
                            //log.trace("res.usl={}, servCd={}", usl, servCd);
                            break;
                        }
                    } else if (munResEl != null) {
                        // Электроэнергия
                        usl = energyUsl;
                    }
                }

                rootEol.setUsl(usl);
                Usl serv = em.find(Usl.class, usl);
                rootEol.setComm(serv.getNm2());

                // найти Ko счетчика, по Ko помещения и коду услуги
                // связывание, пользователь будет сам связывать в Директ
                if (autoBind != null && autoBind) { // fixme проверить что такое autobind у клиентов???
                    soapConfig.saveError(premiseEol, CommonErrs.ERR_EMPTY_KLSK | CommonErrs.ERR_METER_NOT_FOUND, false);
                    if (premiseEol.getKoObj() == null) {
                        log.error("ОШИБКА! По помещению Eolink.id=" + premiseEol.getId() + " не заполнен KLSK! " + " Необходимо произвести экспорт дома Eolink.id=" + houseEol.getId());
                        soapConfig.saveError(premiseEol, CommonErrs.ERR_EMPTY_KLSK, true);
                        //rootEol.setComm("ОШИБКА! По помещению Eolink.id="+premiseEol.getId()+" не заполнен KLSK! " +
                        //        " Необходимо произвести экспорт дома Eolink.id="+houseEol.getId());
                    } else if (usl == null) {
                        throw new ErrorProcessAnswer("Некорректно определён код услуги USL, " + "в методе ulistMng.getUslByResource");
                    } else {
                        Optional<Meter> meter = meterMng.getActualMeterByKo(premiseEol.getKoObj(), usl, new Date());
                        if (meter.isEmpty()) {
                            log.error("ОШИБКА! По помещению Eolink.id={} не найден счетчик usl={}, в карточке Лиц.счета.", premiseEol.getId(), usl);
                            soapConfig.saveError(premiseEol, CommonErrs.ERR_METER_NOT_FOUND, true);
                        } else {
                            // здесь устанавливается именно Ko счетчика, не объекта!
                            if (rootEol.getKoObj() == null) {
                                // только если уже нет привязки!
                                log.trace("Попытка установки KLSK={}, по счетчику Eolink.id={}", meter.get().getKo().getId(), rootEol.getId());
                                rootEol.setKoObj(meter.get().getKo());
                            }
                        }
                    }
                }

                if (isMeterExpPar) {
                    if (rootEol.getKoObj() != null) {
                        // параметры счетчика, если счетчик привязан
                        XMLGregorianCalendar verificationDateCal = basicChar.getFirstVerificationDate();
                        if (verificationDateCal != null) {
                            Date verificationDate = Utl.getDateFromXmlGregCal(verificationDateCal);
                            NsiRef verificationInterval = basicChar.getVerificationInterval();
                            String verificationIntervalCode = verificationInterval.getCode();
                            if (verificationIntervalCode != null) {
                                int intervalYear = Integer.parseInt(verificationIntervalCode);
                                LocalDate verificationLocalDate = LocalDate.ofInstant(verificationDate.toInstant(), ZoneId.systemDefault());
                                LocalDate nextVerificationDate = verificationLocalDate.plusYears(intervalYear);
                                Optional<Meter> meter = meterDAO.getActualByKlskId(rootEol.getKoObj().getId(), new Date());
                                meter.ifPresent(t -> {
                                    ZonedDateTime dateTime = nextVerificationDate.atStartOfDay(ZoneId.systemDefault()).truncatedTo(ChronoUnit.DAYS);
                                    Date endDate = Date.from(dateTime.toInstant());
                                    log.info("По счетчику Meter.id={} установлена дата Окончания работы={}", meter.get().getId(), endDate);
                                    t.setDt2(endDate);
                                });
                            }
                        }
                    }
                }


                eolinkParMng.setStr(rootEol, "Счетчик.НомерПУ", basicChar.getMeteringDeviceNumber());
                eolinkParMng.setStr(rootEol, "Счетчик.Модель", basicChar.getMeteringDeviceModel());
                eolinkParMng.setStr(rootEol, "ПУ.Марка", basicChar.getMeteringDeviceStamp());
                eolinkParMng.setDate(rootEol, "Счетчик.ДатаВводаЭкс", Utl.getDateFromXmlGregCal(basicChar.getCommissioningDate()));
                eolinkParMng.setDate(rootEol, "Счетчик.ДатаУстановки", Utl.getDateFromXmlGregCal(basicChar.getInstallationDate()));
                eolinkParMng.setBool(rootEol, "ГИС ЖКХ.Признак_ПУ_КР", basicChar.isConsumedVolume());

                if (versionEol == null) {
                    // не найдена версия счетчика, создать
                    AddrTp addrTp = lstMng.getAddrTpByCD("СчетчикВерсия");

                    versionEol = Eolink.builder().withGuid(meterType.getMeteringDeviceVersionGUID()).withObjTp(addrTp).withParent(rootEol).withUser(config.getCurUserGis().get()).withStatus(1).build();

                    // пометить прочие записи неактивными
                    eolinkMng.setChildActive(rootEol, "СчетчикВерсия", 0);
                    log.trace("Попытка создать запись версии счетчика в Eolink: GUID={}", meterType.getMeteringDeviceVersionGUID());
                    em.persist(versionEol);
                }
            }
            task.setState("ACP");
            taskMng.logTask(task, false, true);
        }
    }

    private PremiseWithMeter getPremiseWithMeter(MeteringDeviceBasicCharacteristicsType basicChar, Eolink houseEol, ExportMeteringDeviceDataResultType meterType) {
        PremiseWithMeter premiseWithMeter = new PremiseWithMeter();
        if (basicChar.getResidentialPremiseDevice() != null) {
            // Счетчик жилого помещения
            // получить GUID помещения
            premiseWithMeter.meterTp = 0;
            // получить первый элемент (в биллинге Директ, только привязка One to One)
            premiseWithMeter.premiseGUID = basicChar.getResidentialPremiseDevice().getPremiseGUID().get(0);
            log.trace("Cчетчик ЖИЛОГО помещения, GUID={}", premiseWithMeter.premiseGUID);
        } else if (basicChar.getNonResidentialPremiseDevice() != null) {
            // Счетчик нежилого помещения
            // получить GUID помещения
            // получить первый элемент (в биллинге Директ, только привязка One to One)
            premiseWithMeter.premiseGUID = basicChar.getNonResidentialPremiseDevice().getPremiseGUID().get(0);
            log.trace("Cчетчик НЕЖИЛОГО помещения, GUID={}", premiseWithMeter.premiseGUID);
            premiseWithMeter.meterTp = 1;
        } else if (basicChar.getApartmentHouseDevice() != null) {
            log.error("Необрабатываемый тип счетчика - ПУ жилого дома: Root GUID={}", meterType.getMeteringDeviceRootGUID());
            premiseWithMeter.skip = true;
//            continue;
        } else if (basicChar.getCollectiveApartmentDevice() != null) {
            log.error("Необрабатываемый тип счетчика - общеквартирный ПУ " + "(для квартир коммунального заселения): Root GUID={}", meterType.getMeteringDeviceRootGUID());
            premiseWithMeter.skip = true;
//            continue;
        } else if (basicChar.getCollectiveDevice() != null) {
            log.trace("Счетчик - общедомовой ПУ: GUID={}", houseEol.getGuid());
            premiseWithMeter.premiseGUID = houseEol.getGuid();
            premiseWithMeter.meterTp = 2;
        } else if (basicChar.getLivingRoomDevice() != null) {
            log.error("Необрабатываемый тип счетчика - комнатный ПУ " + ": Root GUID={}", meterType.getMeteringDeviceRootGUID());
            premiseWithMeter.skip = true;
//            continue;
        } else {
            // Прочие типы не обрабатывать
            log.error("Необрабатываемый тип счетчика прочего типа: Root GUID={}", meterType.getMeteringDeviceRootGUID());
            premiseWithMeter.skip = true;
//            continue;
        }
        return premiseWithMeter;
    }

    /**
     * Экспортировать данные дома
     */

    public void exportHouseData(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);

        Eolink houseEol = task.getEolink();
        String houseGuid = houseEol.getGuid();

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ExportHouseRequest req = new ExportHouseRequest();
        req.setId("foo");
        par.sb.setSign(true);
        req.setVersion("12.2.0.1");  // здесь просит именно эту версию
        req.setFIASHouseGuid(houseGuid);

        AckRequest ack = null;

        try {
            ack = par.port.exportHouseData(req);
        } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e) {
            e.printStackTrace();
            err = true;
            errMainStr = e.getFaultInfo().getErrorMessage();
        }

        // Показать ошибки, если есть
        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
            taskMng.logTask(task, false, false);
        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
            taskMng.logTask(task, false, true);
        }
    }


    /**
     * Получить результат экспорта объектов дома
     */

    public void exportHouseDataAsk(Integer taskId) throws UnusableCode, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);

        // дом
        Eolink houseEol = task.getEolink();
        // получить состояние
        GetStateResult retState = getState2(task);
        Date curDate = new Date();
        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {
            // Ошибок нет, обработка
            // Сохранить уникальный номер дома
            houseEol.setUn(retState.getExportHouseResult().getHouseUniqueNumber());

            ru.gosuslugi.dom.schema.integration.house_management.ExportHouseResultType.ApartmentHouse apartmentHouse = retState.getExportHouseResult().getApartmentHouse();
            ExportHouseResultType.LivingHouse livingHouse = retState.getExportHouseResult().getLivingHouse();

            if (apartmentHouse != null) {
                // Многоквартирный дом
                // статус - активный
                houseEol.setStatus(1);

                Map<Integer, Eolink> entryMap = new HashMap<>();

                // Подъезды
                List<Eolink> entrances = createEntrance(par, houseEol, curDate, apartmentHouse, entryMap);

                // ЖИЛЫЕ помещения
                createResidentalPremises(par, houseEol, curDate, apartmentHouse, entryMap, entrances);

                // НЕЖИЛЫЕ помещения
                createNonResidentalPremises(par, houseEol, curDate, apartmentHouse);
            } else if (livingHouse != null) {
                log.info("************ Частный дом, houseGUID={}", livingHouse.getHouseGUID()); // todo сделать признак, что Eolink - частный дом
            }
            // Установить статус выполнения задания
            task.setState("ACP");
            //log.info("******* Task.id={}, экспорт объектов дома выполнен", task.getId());
            taskMng.logTask(task, false, true);
        }
    }

    private void createNonResidentalPremises(SoapPar par, Eolink houseEol, Date curDate, ExportHouseResultType.ApartmentHouse apartmentHouse) throws UnusableCode {
        for (NonResidentialPremises t : apartmentHouse.getNonResidentialPremises()) {
            log.trace("Нежилое помещение: №={}, UniqNumber={}, GUID={}, CadastralNumber={}", t.getPremisesNum(), t.getPremisesUniqueNumber(), t.getPremisesGUID(), t.getCadastralNumber());
            Eolink premisEol = eolinkDao2.findEolinkByGuid(t.getPremisesGUID());
            // обработка номера помещения
            String num;
            num = prepNum(t);

            if (premisEol == null) {
                // Не найдено, создать помещение
                AddrTp addrTp = lstMng.getAddrTpByCD("Помещение нежилое");

                premisEol = Eolink.builder().withOrg(par.reqProp.getUk()).withKul(par.reqProp.getKul()).withNd(par.reqProp.getNd()).withKw(num).withGuid(t.getPremisesGUID()).withObjTp(addrTp).withParent(houseEol).withUser(config.getCurUserGis().get()).withCadastrNum(t.getCadastralNumber()).withStatus(1).build();

                log.info("Попытка создать запись Нежилого помещения в Eolink: № помещения={}, un={}, GUID={}", t.getPremisesNum(), t.getPremisesUniqueNumber(), t.getPremisesGUID());
                em.persist(premisEol);
            }

            // погасить ошибки
            soapConfig.saveError(premisEol, CommonErrs.ERR_DIFF_KLSK_BUT_SAME_ADDR | CommonErrs.ERR_EMPTY_KLSK | CommonErrs.ERR_DOUBLE_KLSK_EOLINK, false);

            // обновить параметры помещения
            Date dtTerm = Utl.getDateFromXmlGregCal(t.getTerminationDate());
            if (dtTerm != null && (dtTerm.getTime() < curDate.getTime())) {
                // Объект не активен
                premisEol.setStatus(0);
            } else {
                // Объект активен
                premisEol.setStatus(1);
            }

            // найти соответствующий объект Ko по актуальному помещению
            if (premisEol.getKoObj() == null && premisEol.getStatus().equals(1)) {
                Ko ko = null;
                try {
                    ko = kartMng.getKoPremiseByKulNdKw(par.reqProp.getKul(), par.reqProp.getNd(), num);
                } catch (DifferentKlskBySingleAdress differentKlskBySingleAdress) {
                    // разные KLSK на один адрес!";
                    soapConfig.saveError(premisEol, CommonErrs.ERR_DIFF_KLSK_BUT_SAME_ADDR, true);
                } catch (EmptyId emptyId) {
                    // найден пустой KLSK в данном адресе!";
                    soapConfig.saveError(premisEol, CommonErrs.ERR_EMPTY_KLSK, true);
                }

                if (ko == null) {
                    // вернулся пустой KLSK
                    soapConfig.saveError(premisEol, CommonErrs.ERR_EMPTY_KLSK, true);
                } else {
                    Eolink checkEolink = eolinkDao2.getEolinkByKlskId(ko.getId());
                    if (checkEolink != null) {
                        soapConfig.saveError(premisEol, CommonErrs.ERR_DOUBLE_KLSK_EOLINK, true);
                    } else {
                        //log.info("Попытка установить по объекту Eolink.id={}, Ko.id={}",
                        //      premisEol.getId(), ko.getId());
                        premisEol.setKoObj(ko);
                    }
                }
            } else if (premisEol.getStatus().equals(0)) {
                // убрать Ko по неактуальному помещению
                premisEol.setKoObj(null);
            }
        }
    }

    private void createResidentalPremises(SoapPar par, Eolink houseEol, Date curDate, ExportHouseResultType.ApartmentHouse apartmentHouse, Map<Integer, Eolink> entryMap, List<Eolink> entrances) throws UnusableCode {
        for (ExportHouseResultType.ApartmentHouse.ResidentialPremises t : apartmentHouse.getResidentialPremises()) {
            log.trace("Жилое помещение: №={}, UniqNumber={}, GUID={}, CadastralNumber={}", t.getPremisesNum(), t.getPremisesUniqueNumber(), t.getPremisesGUID(), t.getCadastralNumber());
            Eolink premisEol = eolinkDao2.findEolinkByGuid(t.getPremisesGUID());
            // обработка номера помещения
            String num;
            num = prepNum(t);
            if (premisEol == null) {
                // не найдено, создать помещение
                AddrTp addrTp = lstMng.getAddrTpByCD("Квартира");
                Ko premisKo = null;
                premisEol = Eolink.builder().withOrg(par.reqProp.getUk()).withKul(par.reqProp.getKul()).withNd(par.reqProp.getNd()).withKw(num).withEntry(t.getEntranceNum() != null ? Integer.valueOf(t.getEntranceNum()) : null).withGuid(t.getPremisesGUID()).withUn(t.getPremisesUniqueNumber()).withObjTp(addrTp).withKoObj(premisKo).withParent(t.getEntranceNum() != null ? entryMap.get(Integer.valueOf(t.getEntranceNum())) : houseEol)   // присоединить к родителю:
                        // подъезд, или дом, если не найден подъезд
                        .withUser(config.getCurUserGis().get()).withCadastrNum(t.getCadastralNumber()).withStatus(1).build();
                log.info("Попытка создать запись жилого помещения в Eolink: № подъезда:{}, № помещения={}, un={}, GUID={}", t.getEntranceNum(), t.getPremisesNum(), t.getPremisesUniqueNumber(), t.getPremisesGUID());
                em.persist(premisEol);
            }

            // обновить комнаты
            AddrTp addrTp = lstMng.getAddrTpByCD("Комната");
            for (ExportHouseResultType.ApartmentHouse.ResidentialPremises.LivingRoom r : t.getLivingRoom()) {
                log.trace("Комната, UniqNumber={}, GUID={}, CadastralNumber={} ", r.getLivingRoomUniqueNumber(), r.getLivingRoomGUID(), t.getCadastralNumber());
                Eolink roomEol = eolinkDao2.findEolinkByGuid(r.getLivingRoomGUID());
                if (roomEol == null) {
                    // не найдено, создать комнату
                    roomEol = Eolink.builder().withGuid(r.getLivingRoomGUID()).withUn(r.getLivingRoomUniqueNumber()).withObjTp(addrTp).withKoObj(null) // TODO сделать ko! ред.21.08.2018
                            .withParent(premisEol) // присоединить к квартире
                            .withUser(config.getCurUserGis().get()).withCadastrNum(t.getCadastralNumber()).withStatus(1).build();
                    log.info("Попытка создать запись комнаты в Eolink:un={}, GUID={}", r.getLivingRoomUniqueNumber(), r.getLivingRoomGUID());
                    em.persist(roomEol);
                }
            }

            // погасить ошибки
            soapConfig.saveError(premisEol, CommonErrs.ERR_DIFF_KLSK_BUT_SAME_ADDR | CommonErrs.ERR_EMPTY_KLSK | CommonErrs.ERR_DOUBLE_KLSK_EOLINK | CommonErrs.ERR_NOT_FOUND_ACTUAL_OBJ, false);

            // обновить параметры помещения
            Date dtTerm = Utl.getDateFromXmlGregCal(t.getTerminationDate());
            if (dtTerm != null && (dtTerm.getTime() < curDate.getTime())) {
                // Объект не активен
                premisEol.setStatus(0);
            } else {
                // Объект активен
                premisEol.setStatus(1);
            }
            if (premisEol.getKoObj() == null && premisEol.getStatus().equals(1)) {
                // найти соответствующий объект Ko помещения
                Ko ko = null;
                try {
                    ko = kartMng.getKoPremiseByKulNdKw(par.reqProp.getKul(), par.reqProp.getNd(), num);
                } catch (DifferentKlskBySingleAdress differentKlskBySingleAdress) {
                    // в KART разные KLSK на один адрес!
                    soapConfig.saveError(premisEol, CommonErrs.ERR_DIFF_KLSK_BUT_SAME_ADDR, true);
                } catch (EmptyId emptyId) {
                    // в KART найден пустой KLSK в данном адресе!
                    soapConfig.saveError(premisEol, CommonErrs.ERR_EMPTY_KLSK, true);
                }
                if (ko == null) {
                    // не найден актуальный (действующий объект) в Kart с KLSK
                    soapConfig.saveError(premisEol, CommonErrs.ERR_NOT_FOUND_ACTUAL_OBJ, true);
                } else {
                    Eolink checkEolink = eolinkDao2.getEolinkByKlskId(ko.getId());
                    if (checkEolink != null) {
                        soapConfig.saveError(premisEol, CommonErrs.ERR_DOUBLE_KLSK_EOLINK, true);
                    } else {
                        log.info("Попытка установить по объекту помещения Eolink.id={}, Ko.id={}", premisEol.getId(), ko.getId());
                        premisEol.setKoObj(ko);
                    }
                }
            } else if (premisEol.getStatus().equals(0)) {
                // убрать Ko по неактуальному помещению
                premisEol.setKoObj(null);
            }

            // прикрепить к подъезду, взятому из ГИС ЖКХ
            if (t.getEntranceNum() != null) {
                Integer entryNum = Integer.valueOf(t.getEntranceNum());
                premisEol.setEntry(entryNum);
                // обновить родительский подъезд
                Eolink entry = entrances.stream().filter(e -> e.getEntry().equals(entryNum)).findFirst().orElse(null);
                premisEol.setParent(entry);
            } else {
                // помещение без отдельного входа
                premisEol.setParent(houseEol);
            }

            t.getLivingRoom().forEach(f -> log.trace("f.isNoRSOGKNEGRPRegistered()1={}", f.isNoRSOGKNEGRPRegistered()));
        }
    }

    private List<Eolink> createEntrance(SoapPar par, Eolink houseEol, Date curDate, ExportHouseResultType.ApartmentHouse apartmentHouse, Map<Integer, Eolink> entryMap) {
        List<String> lstEntryGuid = new ArrayList<>();
        for (Entrance t : apartmentHouse.getEntrance()) {
            log.trace("Подъезд: №={}, GUID={}", t.getEntranceNum(), t.getEntranceGUID());
            Eolink entryEol = eolinkDao2.findEolinkByGuid(t.getEntranceGUID());
            lstEntryGuid.add(t.getEntranceGUID());
            if (entryEol == null) {
                // не найдено, создать подъезд
                AddrTp addrTp = lstMng.getAddrTpByCD("Подъезд");

                entryEol = Eolink.builder().withOrg(par.reqProp.getUk()).withKul(par.reqProp.getKul()).withNd(par.reqProp.getNd()).withEntry(Integer.valueOf(t.getEntranceNum())).withGuid(t.getEntranceGUID()).withObjTp(addrTp).withParent(houseEol).withUser(config.getCurUserGis().get()).withStatus(1).build();
                // сохранить, для иерархии
                entryMap.put(Integer.valueOf(t.getEntranceNum()), entryEol);
                em.persist(entryEol);
                // добавить подъезд к дому, чтобы выбирался позже
                houseEol.getChild().add(entryEol);
            }

            // обновить параметры подъезда
            entryEol.setEntry(Integer.valueOf(t.getEntranceNum()));
            Date dtTerm = Utl.getDateFromXmlGregCal(t.getTerminationDate());
            if (dtTerm != null && (dtTerm.getTime() < curDate.getTime())) {
                // Объект не активен
                entryEol.setStatus(0);
            } else {
                // Объект активен
                entryEol.setStatus(1);
            }
        }

        // проверить наличие подъезда по дому, с данным GUID
        List<Eolink> lstEntry = eolinkDao.getChildByTp(houseEol, "Подъезд");
        lstEntry.forEach(t -> {
            log.trace("Подъезд из базы: id={}, entry={}", t.getId(), t.getEntry());
            if (!lstEntryGuid.contains(t.getGuid())) {
                // не найден, промаркировать неактивным
                log.trace("Подъезд №{} помечен неактивным!", t.getEntry());
                t.setStatus(0);
            }
        });
        return lstEntry;
    }

    /**
     * Подготовить номер жилого помещения
     *
     * @param t
     */
    private String prepNum(ru.gosuslugi.dom.schema.integration.house_management.ExportHouseResultType.ApartmentHouse.ResidentialPremises t) {
        String num;
        // усечь № кв. до 7 знаков
        if (t.getPremisesNum().length() > 7) {
            num = t.getPremisesNum().substring(0, 7);
        } else {
            num = t.getPremisesNum();
        }
        // добавить лидирующие нули
        num = Utl.lpad(num, "0", 7);
        return num;
    }

    /**
     * Подготовить номер нежилого помещения
     *
     * @param t
     */
    private String prepNum(ru.gosuslugi.dom.schema.integration.house_management.ExportHouseResultType.ApartmentHouse.NonResidentialPremises t) {
        String num;
        // усечь № кв. до 7 знаков
        if (t.getPremisesNum().length() > 7) {
            num = t.getPremisesNum().substring(0, 7);
        } else {
            num = t.getPremisesNum();
        }
        // добавить лидирующие нули
        num = Utl.lpad(num, "0", 7);
        return num;
    }

    /**
     * Экспортировать лицевые счета
     */

    public void exportAccountData(Integer taskId) throws CantPrepSoap, CantSendSoap, WrongParam {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        if (task.getProcUk() == null)
            throw new WrongParam("По заданию task.id=" + task.getId() + " не заполнен TASK.FK_PROC_UK");
        // индивидуально выполнить setUp - так как может выполняться от имени РСО
        setUp(task);

        // Установить параметры SOAP
        SoapPar par = setUp(task);

        ExportAccountRequest req = new ExportAccountRequest();
        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        // GUID дома
        req.setFIASHouseGuid(task.getEolink().getGuid());

        AckRequest ack = null;
        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;
        try {
            ack = par.port.exportAccountData(req);
        } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e1) {
            err = true;
            e1.printStackTrace();
            errMainStr = e1.getFaultInfo().getErrorMessage();
        }

        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
            taskMng.logTask(task, false, false);
        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
            taskMng.logTask(task, false, true);
        }
    }

    /**
     * Получить результат экспорта лицевых счетов
     */

    public void exportAccountDataAsk(Integer taskId) throws CantPrepSoap, WrongParam, ErrorProcessAnswer, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        if (task.getProcUk() == null)
            throw new WrongParam("По заданию task.id=" + task.getId() + " не заполнен TASK.FK_PROC_UK");
        // индивидуально выполнить setUp - так как может выполняться от имени РСО
        setUp(task);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
            return;
        } else if (!task.getState().equals("ERR")) {
            // ошибок не найдено
            for (ExportAccountResultType t : retState.getExportAccountResult()) {

                // примечание по объекту
                String guid = null;
                for (ru.gosuslugi.dom.schema.integration.house_management.AccountExportType.Accommodation d : t.getAccommodation()) {
                    if (d.getPremisesGUID() != null) {
                        // Лиц счет на помещение
                        guid = d.getPremisesGUID();
                    } else if (d.getLivingRoomGUID() != null) {
                        // Лиц счет на комнату
                        guid = d.getLivingRoomGUID();
                    } else {
                        // Лиц счет на дом
                        guid = d.getFIASHouseGuid();
                    }
                    log.trace("лиц.счет={}", t.getAccountNumber());
                    for (AccountReasonsImportType.TKOContract tkoContract : t.getAccountReasons().getTKOContract()) {
                        log.trace("Основание ТКО лиц.счета Contract GUID={}", tkoContract.getContractGUID());
                    }

                    for (AccountReasonsImportType.SupplyResourceContract suppContr : t.getAccountReasons().getSupplyResourceContract()) {
                        log.trace("Основание лиц.счета Contract GUID={}", suppContr.getContractGUID());
                    }
                }

                // найти лицевой счет
                Eolink lskEol = eolinkDao2.findEolinkByGuid(t.getAccountGUID());
                String num;
                // усечь № лиц.счета до 8 знаков
                if (t.getAccountNumber().length() > 8) {
                    num = t.getAccountNumber().substring(0, 8);
                } else {
                    num = t.getAccountNumber();
                }

                // Найти лицевой счет в Kart
                Kart kart = em.find(Kart.class, num);
                // установить ЕЛС в Kart, для упрощения выборок и быстрой визуализации ЕЛС в карточке, так же для формирования долгов для Сбера
                if (kart != null && kart.getElsk() == null) {
                    kart.setElsk(t.getUnifiedAccountNumber());
                }
                if (lskEol == null) {
                    // Создать новый лицевой счет

                    // Найти объект на который ссылаться
                    Eolink parentEol = eolinkDao2.findEolinkByGuid(guid);
                    if (parentEol == null) {
                        log.warn("Не найдено помещение c GUID=" + guid + ", для прикрепления лицевого счета, " + "попробуйте выполнить экспорт объектов дома!");
                    } else {
                        if (kart == null) {
                            log.error("ОШИБКА! Не найден лиц.счет в SCOTT.KART c lsk=" + num);
                        } else {
                            AddrTp addrTp = lstMng.getAddrTpByCD("ЛС");

                            lskEol = Eolink.builder().withGuid(t.getAccountGUID()).withUn(t.getUnifiedAccountNumber()) // ЕЛС
                                    .withServiceId(t.getServiceID()) // идентификатор ЖКУ
                                    .withKart(kart).withObjTp(addrTp).withParent(parentEol).withUk(task.getProcUk()).withOrg(task.getProcUk().getOrg()).withUser(config.getCurUserGis().get()).withStatus(1).build();
                            log.info("Попытка создать запись лицевого счета в Eolink: GUID={}, AccountNumber={}, ServiceId={}", t.getAccountGUID(), num, t.getServiceID());
                            em.persist(lskEol);
                        }
                    }
                } else {
                    // Лиц.счет уже существует, обновить его параметры
                    LskEolParam lskEolParam = new LskEolParam();
                    lskEolParam.setAccountGUID(t.getAccountGUID());
                    lskEolParam.setAccountNumber(t.getAccountNumber());
                    lskEolParam.setUnifiedAccountNumber(t.getUnifiedAccountNumber());
                    lskEolParam.setServiceID(t.getServiceID());
                    lskEolParam.setEolink(lskEol);
                    lskEolParam.setIsClosed(t.getClosed() != null);
                    updateLskEol(lskEolParam);
                }

            }
            task.setState("ACP");
            taskMng.logTask(task, false, true);
        }
    }


    /**
     * Обновить параметры лиц.счета
     *
     * @param param - параметры
     */
    private void updateLskEol(LskEolParam param) {
        log.trace("Попытка обновить запись лицевого счета в Eolink: GUID={}, AccountNumber={}, " + "UnifiedAccountNumber={}, ServiceId={}", param.getAccountGUID(), param.getAccountNumber(), param.getUnifiedAccountNumber(), param.getServiceID());
        // GUID
        if (param.getEolink().getGuid() == null) {
            param.getEolink().setGuid(param.getAccountGUID());
        }
        // ЕЛС
        if (param.getEolink().getUn() == null) {
            param.getEolink().setUn(param.getUnifiedAccountNumber());
        } else {
            if (!param.getEolink().getUn().equals(param.getUnifiedAccountNumber())) {
                log.warn("ВНИМАНИЕ! Изменился ЕЛС лиц.счета по Eolink.id={}," + "UnifiedAccountNumber был={}, стал={}", param.getEolink().getId(), param.getEolink().getUn(), param.getUnifiedAccountNumber());
                param.getEolink().setUn(param.getUnifiedAccountNumber());
            }
        }
        // идентификатор ЖКУ
        if (param.getEolink().getServiceId() == null) {
            param.getEolink().setServiceId(param.getServiceID());
        }
        // отметить открытый или закрытый лс
        if (param.getIsClosed() != null) {
            if (param.getIsClosed()) {
                param.getEolink().setStatus(0);
            } else {
                param.getEolink().setStatus(1);
            }
        }
    }

    /**
     * Импортировать лицевые счета
     */

    public void importAccountData(Integer taskId) throws CantPrepSoap, CantSendSoap, WrongParam, UnusableCode {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        if (task.getProcUk() == null)
            throw new WrongParam("По заданию task.id=" + task.getId() + " не заполнен TASK.FK_PROC_UK");
        // индивидуально выполнить setUp - так как может выполняться от имени РСО
        SoapPar par = setUp(task);

        // Установить параметры SOAP

        ImportAccountRequest req = new ImportAccountRequest();
        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());

        // использовать ли фильтр, для проверки XML
        boolean isUseFilter = task.getIdFrom() != null && task.getIdTo() != null;
        if (isUseFilter) {
            log.info("Будет использован фильтр idFrom={}, idTo={}", task.getIdFrom(), task.getIdTo());
        }
        Eolink houseEol = task.getEolink();

        // создать отсутствующие в EOLINK объекты лицевых счетов по данному Дому и УК из KART
        List<Kart> lstKartAbsent = eolinkMng.getKartNotExistsInEolink(houseEol.getId(), task.getProcUk().getId());
        if (lstKartAbsent.size() > 0) {
            log.info("Кол-во лиц.счетов на загрузку {}", lstKartAbsent.size());
            for (Kart kart : lstKartAbsent) {
                log.info("Попытка создать лиц.счет в EOLINK, lsk={}", kart.getLsk());
                Eolink eolKw = eolinkDao2.getEolinkByKlskId(kart.getKoPremise().getId());
                if (eolKw == null) {
                    log.error("ОШИБКА! По лиц.счету KART.LSK={} не найден объект типа 'Квартира' в EOLINK, c помощью K_LSK_ID={}", kart.getLsk(), kart.getKoKw().getId());
                } else {
                    AddrTp objTp = addrTpDAO.getByCd("ЛС");
                    Eolink eolKart = Eolink.builder().withUk(task.getProcUk()).withObjTp(objTp).withStatus(1).withParent(eolKw).withOrg(kart.getUk()).withKart(kart).build();
                    em.persist(eolKart);
                }
            }
        }
        // получить лиц.счета для добавления/обновления в ГИС.
        List<Eolink> lstLskForUpdateBeforeSorting = new ArrayList<>(eolinkMng.getLskEolByHouseEol(houseEol.getId(), task.getProcUk().getId())).stream().filter(t -> !isUseFilter || Utl.between(t.getId(), task.getIdFrom(), task.getIdTo())).collect(Collectors.toList());
        // упорядочить по Eolink.id, найти элементы для обновления с большим ID, чем было обработано до этого
        List<Eolink> lstEolinkForUpdate = lstLskForUpdateBeforeSorting.stream().filter(t -> task.getEolinkLast() == null || t.getId().compareTo(task.getEolinkLast().getId()) > 0).sorted(Comparator.comparing(Eolink::getId)).collect(Collectors.toList());

        int i = 0;
        Eolink lastLskEol = null;
        for (Eolink lskEol : lstEolinkForUpdate) {
            log.info("Проверка для загрузки лиц счета: EOLINK.ID={}", lskEol.getId());
            Kart kart = lskEol.getKart();
            // погасить ошибки и комментарии
            soapConfig.saveError(lskEol, CommonErrs.ERR_IMPORT | CommonErrs.ERR_LSK_NOT_FOUND | CommonErrs.ERR_INCORRECT_PARENT | CommonErrs.ERR_EMPTY_FIO, false);
            lskEol.setComm(null);

            if (kart == null) {
                // не найден лиц.счет
                log.error("Объект лиц.счета EOLINK.ID={}, не найден в SCOTT.KART по LSK", lskEol.getId());
                soapConfig.saveError(lskEol, CommonErrs.ERR_LSK_NOT_FOUND, true);
            } else if (lskEol.getParent().getStatus() == 0) {
                log.error("Объект лиц.счета EOLINK.ID={}, не будет обновлен в ГИС, так как ссылается на неактуальный " + "родительский объект", lskEol.getId());
            } else if (kart.getKIm() == null || kart.getKFam() == null || kart.getKOt() == null) {
                // не заполнены ФИО собственника в SCOTT.KART
                soapConfig.saveError(lskEol, CommonErrs.ERR_EMPTY_FIO, true);
                log.error("По лиц.счету LSK={} не заполнены Ф.И.О., загрузка не возможна!", lskEol.getId());
            } else {
                log.info("Обработка лиц счета: EOLINK.ID={}, KART.LSK={}", lskEol.getId(), kart.getLsk());
                i++;
                if (i > 100) {
                    // сохранить последний обработанный лиц.счет Eolink, выйти из цикла
                    task.setEolinkLast(lastLskEol);
                    break;
                } else {
                    lastLskEol = lskEol;
                }

                ImportAccountRequest.Account ac = new ImportAccountRequest.Account();
                req.getAccount().add(ac);

                String reu = lskEol.getUk().getOrg().getReu();
                if (reu == null) {
                    throw new WrongParam("Не заполнен код REU в организации EOLINK.ID=" + lskEol.getUk().getId());
                }
                Org uk = orgDAO.getByReu(reu);
                if (uk == null) {
                    throw new WrongParam("По коду REU=" + reu + " не найдена организация в справочнике T_ORG");
                }
                if (uk.isUO()) {
                    // лиц.счет УК
                    ac.setIsUOAccount(true);
                } else if (uk.isTKO()) {
                    // лиц.счет ТКО
                    ac.setIsTKOAccount(true);

                    /*  note пока не убирать код в комментарии - понадобится ред. 19.08.2019
                    // основания лиц.счета
                    AccountReasonsImportType reason = new AccountReasonsImportType();
                    AccountReasonsImportType.TKOContract tkoContract = new AccountReasonsImportType.TKOContract();
                    tkoContract.setContractGUID("8c9cd2af-26b1-417d-b0ff-7a5cff1f1433");
                    reason.getTKOContract().add(tkoContract);
                    ac.setAccountReasons(reason);
                */
                } else if (uk.isRSO()) {
                    // лиц.счет РСО
                    ac.setIsRSOAccount(true);
                } else {
                    throw new WrongParam("ОШИБКА! Неподдерживаемый тип организации SCOTT.T_ORG.ORG_TP_GIS=" + uk.getOrgTpGis() + ", SCOTT.T_ORG.ID=" + uk.getId());
                }

                ac.setLivingPersonsNumber(Utl.nvl(kart.getKpr(), 0));
                ac.setTotalSquare(Utl.nvl(kart.getOpl(), BigDecimal.ZERO));
                ac.setHeatedArea(Utl.nvl(kart.getOpl(), BigDecimal.ZERO));
                log.info("Будет обновлено:");
                log.info("кол-во прожив. = {}", ac.getLivingPersonsNumber());
                log.info("общая площадь = {}", ac.getTotalSquare());
                log.info("отапливаемая площадь = {}", ac.getHeatedArea());

                // транспортный GUID
                String tguid = Utl.getRndUuid().toString();
                lskEol.setTguid(tguid);
                ac.setTransportGUID(tguid);

                // привязка к помещению или к дому
                AccountType.Accommodation acm = new AccountType.Accommodation();
                ac.getAccommodation().add(acm);
                if (lskEol.getParent().getObjTp().getCd().equals("Квартира") || lskEol.getParent().getObjTp().getCd().equals("Дом") || lskEol.getParent().getObjTp().getCd().equals("Помещение нежилое")) {
                    acm.setPremisesGUID(lskEol.getParent().getGuid());
                } else {
                    log.error("Объект лицевого счета EOLINK.ID={} имеет некорректную родительскую запись с типом={}, " + "разрешённые типы: 'Квартира', 'Дом', 'Помещение нежилое'", lskEol.getId(), lskEol.getParent().getObjTp().getCd());
                    soapConfig.saveError(lskEol, CommonErrs.ERR_INCORRECT_PARENT, true);
                    continue;
                }

                // № лицевого счета
                ac.setAccountNumber(kart.getLsk());

                // Сведения о плательщике
                AccountType.PayerInfo pf = new AccountType.PayerInfo();
                AccountIndType ind = new AccountIndType();

                // ред. 11.10.2019 - ФИО является персональными данными, не передаются,
                // только в случае разделенных лиц.счетов. - задал вопрос СКЭК по Полысаево, почему не передаётся?
                ind.setFirstName(kart.getKIm());
                ind.setSurname(kart.getKFam());
                ind.setPatronymic(kart.getKOt());

                // наниматель?
                if (kartMng.getIsRenter(kart)) {
                    pf.setIsRenter(true);
                }

                // лиц.счет разделен?
                if (kart.getIsDivided()) {
                    markAsDivided(kart, pf, ind);
                } else {
                    // установка ФИО в лиц.счет. ред.28.01.2020
                    pf.setInd(ind);
                }

                ac.setPayerInfo(pf);

                if (lskEol.getGuid() != null) {
                    // Account GUID, только при обновлении лиц.счета
                    ac.setAccountGUID(lskEol.getGuid());
                }

                // закрытый лиц.счет
                Optional<StateSch> stateSchOpt = kartMng.getKartStateByDate(kart, new Date());
                if (stateSchOpt.isPresent()) {
                    StateSch stateSch = stateSchOpt.get();
                    if (stateSch.getReason() != null) {
                        // заполнена причина закрытия лиц.счета в ГИС ЖКХ
                        // если дата закрытия пустая - проставить первую дату месяца
                        Date stateSchDt = stateSch.getDt1() == null ? Utl.getFirstDate(new Date()) : stateSch.getDt1();

                        // причина закрытия лиц.счета
                        ClosedAccountAttributesType closedAttributes = new ClosedAccountAttributesType();
                        String reasonGuid = stateSch.getReason().getGuid();
                        Ulist reasonUlist = ulistDAO.getListElemByGUID(reasonGuid);
                        NsiRef reasonNsiElem = ulistMng.getNsiElem(reasonUlist);
                        closedAttributes.setCloseReason(reasonNsiElem);
                        try {
                            closedAttributes.setCloseDate(Utl.getXMLDate(stateSchDt));
                        } catch (DatatypeConfigurationException e) {
                            throw new WrongParam("Некорректная дата закрытия лиц счета lsk=" + kart.getLsk());
                        }
                        ac.setClosed(closedAttributes);
                        // установить статус - закрытый в Eolink
                        lskEol.setStatus(0);
                    }
                }
            }
        }

        if (i > 0 && i <= 100) {
            // было обработано <= 100 лиц.счетов, убрать последний объект, для исключения повторного вызова в методе Ask
            task.setEolinkLast(null);
        }

        if (i > 0) {
            log.info("******* Task.id={}, импорт лиц.счетов по дому, вызов", task.getId());
            AckRequest ack = null;
            // для обработки ошибок
            boolean err = false;
            String errMainStr = null;
            try {
                ack = par.port.importAccountData(req);
            } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e1) {
                e1.printStackTrace();
                err = true;
                errMainStr = e1.getFaultInfo().getErrorMessage();
            }

            if (err) {
                task.setState("ERR");
                task.setResult("Ошибка при отправке XML: " + errMainStr);
                taskMng.logTask(task, false, false);
            } else {
                // Установить статус "Запрос статуса"
                task.setState("ACK");
                task.setMsgGuid(ack.getAck().getMessageGUID());
                taskMng.logTask(task, false, true);
            }
        } else {
            // Установить статус "Выполнено", так как нечего загружать
            log.info("Task.id={}, Нет лиц.счетов для загрузки!", task.getId());
            task.setEolinkLast(null);
            task.setState("ACP");
        }
    }

    /**
     * Установить реквизиты для разделенного лиц.счета
     *
     * @param kart - лиц.счет
     * @param pf   - информация о плательщике
     * @param ind  - информация о физ.лице // todo сделать так же об организации, в случае разделенного по Орг. ЕЛС
     */
    private void markAsDivided(Kart kart, AccountType.PayerInfo pf, AccountIndType ind) {
        // разделенный лиц.счет, найти владельца с заполненными документами
        List<KartPr> lstKartPr = kart.getKartPr().stream().filter(KartPr::getIsUseDividedEls).collect(Collectors.toList());
        if (lstKartPr.size() > 1) {
            log.error("ОШИБКА! Лиц.счет помечен как разделенный, " + "у больше чем одного собственника отмеченно использовать " + "документы проживающего для разделенного ЕЛС");
        } else if (lstKartPr.size() == 0) {
            log.error("ОШИБКА! Лиц.счет помечен как разделенный, " + "ни у одного собственника не отмеченно использовать " + "документы проживающего для разделенного ЕЛС");
        } else if (!lstKartPr.get(0).isUseDocForDividedEls()) {
            log.error("ОШИБКА! Лиц.счет помечен как разделенный, " + "отмеченно использовать документы проживающего для разделенного ЕЛС," + "а сами документы - пустые");
        } else {
            KartPr kartPr = lstKartPr.get(0);
            // всё ОК
            pf.setIsAccountsDivided(true);
            if (kartPr.getSnils() != null) {
                // по СНИЛС
                ind.setSNILS(kartPr.getSnils());
            } else {
                // по другому документу
                ID id = new ID();
                id.setSeries(kartPr.getDocSeries());
                id.setNumber(kartPr.getDocNumber());

                NsiRef idType = ulistMng.getNsiElem(kartPr.getDocTp().getUlist());
                //NsiRef idType = ulistMng.getNsiElem("NSI", 95, "Вид документа, удостоверяющего личность",
                //       "Паспорт гражданина Российской Федерации");
                id.setType(idType);
                try {
                    id.setIssueDate(Utl.getXMLDate(Utl.getDateFromStr("12.12.2013")));
                } catch (DatatypeConfigurationException | ParseException e) {
                    e.printStackTrace();
                }
                ind.setID(id);
            }
            pf.setInd(ind);
        }
    }


    /**
     * Получить результат импорта лицевых счетов
     */

    public void importAccountDataAsk(Integer taskId) throws CantPrepSoap, WrongParam, CantSendSoap, UnusableCode {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        if (task.getProcUk() == null)
            throw new WrongParam("По заданию task.id=" + task.getId() + " не заполнен TASK.FK_PROC_UK");
        // индивидуально выполнить setUp - так как может выполняться от имени РСО
        setUp(task);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR")) {
            // ошибок не найдено
            for (GetStateResult.ImportResult t : retState.getImportResult()) {
                log.trace("После импорта объектов получены следующие параметры:");
                for (GetStateResult.ImportResult.CommonResult d : t.getCommonResult()) {

                    // найти элемент лиц.счета по Транспортному GUID
                    Eolink lskEol = eolinkDao2.findEolinkByTguid(d.getTransportGUID());
                    // погасить ошибки
                    soapConfig.saveError(lskEol, CommonErrs.ERR_IMPORT, false);

                    // ошибки внутри выполненного задания
                    for (Error f : d.getError()) {
                        String errStr = String.format("Ошибка импорта лиц.счета в ГИС ЖКХ: " + "Error code=%s, Description=%s", f.getErrorCode(), f.getDescription());
                        soapConfig.saveError(lskEol, CommonErrs.ERR_IMPORT, true);
                        lskEol.setComm(errStr);
                        log.error(errStr);
                    }

                    if (d.getImportAccount() != null) {
                        if (lskEol != null) {
                            LskEolParam lskEolParam = new LskEolParam();
                            lskEolParam.setAccountGUID(d.getGUID());
                            lskEolParam.setUnifiedAccountNumber(d.getImportAccount().getUnifiedAccountNumber());
                            lskEolParam.setServiceID(d.getImportAccount().getServiceID());
                            lskEolParam.setEolink(lskEol);
                            updateLskEol(lskEolParam);

                        } else {
                            log.error("ОШИБКА! Не найден лиц.счет в Eolink по TGUD={}", d.getTransportGUID());
                        }
                    }
                }
            }
            if (task.getEolinkLast() != null) {
                log.info("******* Task.id={}, Импорт части лиц.счетов дома выполнен, " + "будет произведён повтор задания, state='INS'", task.getId());
                task.setState("INS");
                taskMng.logTask(task, false, true);
            } else {
                task.setState("ACP");
                taskMng.logTask(task, false, true);
            }
        }
    }


    /*
     * Обновление объектов дома
     *
     */

    public void importHouseUOData(Integer taskId) throws WrongGetMethod, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        //log.info("******* Task.id={}, Импорт объектов дома, вызов", task.getId());
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);
        ImportHouseUORequest req = new ImportHouseUORequest();
        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        //sb.setSign(true);

        ApartmentHouse ah = new ApartmentHouse();

        // установить базовые параметры
        if (task.getAct().getCd().equals("GIS_ADD_HOUSE")) { // Эта ветка почему то не работает, работает только обновление
            // Добавить дом
            log.trace("Добавление дома, Task.id={}", task.getId());
            BasicCharacteristicts bc = new BasicCharacteristicts();
            bc.setFIASHouseGuid(task.getEolink().getGuid());
            Double totSqr = teParMng.getDbl(task, "ГИС ЖКХ.Общая площадь жилых помещений по паспорту помещения");
            bc.setTotalSquare(BigDecimal.valueOf(totSqr));

            Date dtBuild = teParMng.getDate(task, "ГИС ЖКХ.Дата постройки");
            String dtBuiltStr = Utl.getStrFromDate(dtBuild);
            dtBuiltStr = dtBuiltStr.substring(dtBuiltStr.length() - 4);
            Integer usedYear = Integer.valueOf(dtBuiltStr);
            bc.setUsedYear(BigDecimal.valueOf(usedYear).shortValue());

            Double cult = teParMng.getDbl(task, "ГИС ЖКХ.Наличие статуса объекта культурного наследия");
            bc.setCulturalHeritage(cult == 1d);

            // установить часовую зону
            bc.setOlsonTZ(ulistMng.getNsiElem("NSI", 32/*30*/, "Часовая зона", "Asia/Novokuznetsk")); // ред.28.12.17 странно было 31 поменял на 32

            Double et = teParMng.getDbl(task, "Количество этажей, наибольшее(1-11)");
            bc.setFloorCount(et.byteValue());

            Boolean isGkn = teParMng.getBool(task, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
            if (isGkn != null && isGkn) {
                // Ключ связи с ГКН/ЕГРП отсутствует.
                bc.setNoRSOGKNEGRPRegistered(true);
            } else {
                // Ключ связи с ГКН/ЕГРП присутствует, поставить номер ГКН
                String gknKey = teParMng.getStr(task, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                if (gknKey == null) {
                    throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + task.getId());
                }
                bc.setCadastralNumber(gknKey);
            }

            // установить ОКТМО
            OKTMORefType oktmo = new OKTMORefType();
            String oktmo2 = teParMng.getStr(task, "ГИС ЖКХ.ОКТМО");

            oktmo.setCode(oktmo2);
            oktmo.setName(oktmo2);
            bc.setOKTMO(oktmo);

            // установить состояние объекта
            ApartmentHouseToCreate ac = new ApartmentHouseToCreate();
            String state = teParMng.getStr(task, "ГИС ЖКХ.Состояние");
            bc.setState(ulistMng.getNsiElem("NSI", 24, "Состояние дома", state));

            Double underEt = teParMng.getDbl(task, "ГИС ЖКХ.Количество подземных этажей");
            ac.setUndergroundFloorCount(underEt.byteValue());

            Double etMin = teParMng.getDbl(task, "Количество этажей, наименьшее(1-10)");
            log.trace("etMin={}", etMin);
            Integer etMin2 = etMin.intValue();
            ac.setMinFloorCount(etMin2.byteValue());

            String tguid = Utl.getRndUuid().toString();
            task.setTguid(tguid);
            ac.setTransportGUID(tguid);

            ac.setBasicCharacteristicts(bc);
            ah.setApartmentHouseToCreate(ac);

        } else if (task.getAct().getCd().equals("GIS_UPD_HOUSE")) {
            // Обновить дом
            log.trace("Обновление дома, Task.id={}", task.getId());
            HouseBasicUpdateUOType bc = new HouseBasicUpdateUOType();
            bc.setFIASHouseGuid(task.getEolink().getGuid());
            Double totSqr = teParMng.getDbl(task, "ГИС ЖКХ.Общая площадь жилых помещений по паспорту помещения");
            bc.setTotalSquare(BigDecimal.valueOf(totSqr));

            Date dtBuild = teParMng.getDate(task, "ГИС ЖКХ.Дата постройки");
            String dtBuiltStr = Utl.getStrFromDate(dtBuild);
            dtBuiltStr = dtBuiltStr.substring(dtBuiltStr.length() - 4);
            int usedYear = Integer.parseInt(dtBuiltStr);
            bc.setUsedYear(BigDecimal.valueOf(usedYear).shortValue());

            Double cult = teParMng.getDbl(task, "ГИС ЖКХ.Наличие статуса объекта культурного наследия");
            bc.setCulturalHeritage(cult == 1d);

            // установить часовую зону
            bc.setOlsonTZ(ulistMng.getNsiElem("NSI", 32/*30*/, "Часовая зона", "Asia/Novokuznetsk")); //TODO проверить почему стояло 30, когда часовая зона по OLSON это 31

            Double et = teParMng.getDbl(task, "Количество этажей, наибольшее(1-11)");
            bc.setFloorCount(et.byteValue());

            Boolean isGkn = teParMng.getBool(task, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
            if (isGkn != null && isGkn) {
                // Ключ связи с ГКН/ЕГРП отсутствует.
                bc.setNoRSOGKNEGRPRegistered(true);
            } else {
                String gknKey = teParMng.getStr(task, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                if (gknKey == null) {
                    throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + task.getId());
                }
                bc.setCadastralNumber(gknKey);
            }

            // установить ОКТМО
            OKTMORefType oktmo = new OKTMORefType();
            String oktmo2 = teParMng.getStr(task, "ГИС ЖКХ.ОКТМО");

            oktmo.setCode(oktmo2);
            oktmo.setName(oktmo2);
            bc.setOKTMO(oktmo);

            // установить состояние объекта
            String state = teParMng.getStr(task, "ГИС ЖКХ.Состояние");
            bc.setState(ulistMng.getNsiElem("NSI", 24, "Состояние дома", state));

            ApartmentHouseToUpdate ac = new ApartmentHouseToUpdate();

            Double underEt = teParMng.getDbl(task, "ГИС ЖКХ.Количество подземных этажей");
            ac.setUndergroundFloorCount(underEt.byteValue());

            Double etMin = teParMng.getDbl(task, "Количество этажей, наименьшее(1-10)");
            int etMin2 = etMin.intValue();
            ac.setMinFloorCount((byte) etMin2);

            String tguid = Utl.getRndUuid().toString();
            task.setTguid(tguid);
            log.trace("Установлен house TGUID={}", tguid);
            ac.setTransportGUID(tguid);

            ac.setBasicCharacteristicts(bc);
            ah.setApartmentHouseToUpdate(ac);
        }

        // Добавить подъезды
        taskDao.getByTaskAddrTp(task, "Подъезд", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_ADD_ENTRY")) // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Добавление подъезда, Task.id={}", t.getId());
                    EntranceToCreate ec = new EntranceToCreate();
                    String entryNum = String.valueOf(t.getEolink().getEntry());
                    ec.setEntranceNum(entryNum);

                    // год постройки
                    Date dtEntrBuild = teParMng.getDate(t, "ГИС ЖКХ.Дата постройки");
                    String dtEntrBuiltStr = Utl.getStrFromDate(dtEntrBuild);
                    dtEntrBuiltStr = dtEntrBuiltStr.substring(dtEntrBuiltStr.length() - 4);
                    ec.setCreationYear(Short.valueOf(dtEntrBuiltStr));

                    // этажность
                    Double etEntr = teParMng.getDbl(t, "Количество этажей, наибольшее(1-11)");
                    ec.setStoreysCount(etEntr.byteValue());

                    // Трансп. GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    ec.setTransportGUID(t.getTguid());

                    ah.getEntranceToCreate().add(ec);
                }));
        // Обновить подъезды
        taskDao.getByTaskAddrTp(task, "Подъезд", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_UPD_ENTRY")) // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Обновление подъезда, Task.id={}, Guid={}", t.getId(), t.getEolink().getGuid());
                    EntranceToUpdate eu = new EntranceToUpdate();
                    eu.setEntranceGUID(t.getEolink().getGuid());
                    String entryNum = String.valueOf(t.getEolink().getEntry());
                    eu.setEntranceNum(entryNum);

                    // год постройки
                    Date dtEntrBuild = teParMng.getDate(t, "ГИС ЖКХ.Дата постройки");

                    String dtEntrBuiltStr = Utl.getStrFromDate(dtEntrBuild);
                    dtEntrBuiltStr = dtEntrBuiltStr.substring(dtEntrBuiltStr.length() - 4);
                    eu.setCreationYear(Short.valueOf(dtEntrBuiltStr));

                    // этажность
                    Double etEntr = teParMng.getDbl(t, "Количество этажей, наибольшее(1-11)");
                    eu.setStoreysCount(etEntr.byteValue());

                    // Трансп. GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    eu.setTransportGUID(t.getTguid());
                    ah.getEntranceToUpdate().add(eu);
                }));

        // Добавить жилое помещение(ия)
        taskDao.getByTaskAddrTp(task, "Квартира", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_ADD_PRMS")) // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Добавление жилого помещения, Task.id={}", t.getId());
                    ResidentialPremises rp = new ResidentialPremises();
                    ResidentialPremisesToCreate rc = new ResidentialPremisesToCreate();

                    // Тип - отдельная квартира
                    rc.setPremisesCharacteristic(ulistMng.getNsiElem("NSI", 30, "Характеристика помещения", "Отдельная квартира"));

                    Boolean isGkn = teParMng.getBool(t, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
                    if (isGkn != null && isGkn) {
                        // Ключ связи с ГКН/ЕГРП отсутствует.
                        rc.setNoRSOGKNEGRPRegistered(true);
                    } else {
                        String gknKey = teParMng.getStr(t, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                        if (gknKey == null) {
                            throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + t.getId());
                        }
                        // Ключ связи с ГКН/ЕГРП присутствует, поставить номер ГКН
                        rc.setCadastralNumber(gknKey);
                    }

                    // наличие подъезда
                    if (t.getEolink().getParent().getObjTp().getCd().equals("Подъезд")) {
                        // есть подъезд
                        // номер подъезда
                        String entryNum = String.valueOf(t.getEolink().getEntry());
                        if (entryNum != null) {
                            rc.setEntranceNum(entryNum);
                        }
                    } else {
                        // нет подъезда
                        rc.setHasNoEntrance(true);
                    }

                    // Номер помещения
                    rc.setPremisesNum(Utl.ltrim(t.getEolink().getKw(), "0"));
                    // Общая площадь
                    Double totalArea = teParMng.getDbl(t, "Площадь.Общая");
                    rc.setTotalArea(BigDecimal.valueOf(totalArea));

                    // Жилая площадь
                    Double grossArea = teParMng.getDbl(t, "Площадь.Жилая");
                    if (grossArea != null) {
                        rc.setGrossArea(BigDecimal.valueOf(grossArea));
                    } else {
                        rc.setNoGrossArea(true);
                    }

                    // Транспортный GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    rc.setTransportGUID(t.getTguid());

                    rp.setResidentialPremisesToCreate(rc);
                    ah.getResidentialPremises().add(rp);
                }));

        // Добавить НЕжилое помещение(ия)
        taskDao.getByTaskAddrTp(task, "Помещение нежилое", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_ADD_PRMS")) // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Добавление НЕжилого помещения, Task.id={}", t.getId());
                    NonResidentialPremiseToCreate rc = new NonResidentialPremiseToCreate();

                    Boolean isGkn = teParMng.getBool(t, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
                    if (isGkn != null && isGkn) {
                        // Ключ связи с ГКН/ЕГРП отсутствует.
                        rc.setNoRSOGKNEGRPRegistered(true);
                    } else {
                        // Ключ связи с ГКН/ЕГРП присутствует, поставить номер ГКН
                        String gknKey = teParMng.getStr(t, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                        if (gknKey == null) {
                            throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + t.getId());
                        }
                        rc.setCadastralNumber(gknKey);
                    }
                    // Номер помещения
                    rc.setPremisesNum(Utl.ltrim(t.getEolink().getKw(), "0"));

                    String commProp = teParMng.getStr(t, "Помещение, сост.общ.имущ.МКД");
                    rc.setIsCommonProperty(commProp.equals("Да"));

                    // Общая площадь
                    Double totalArea = teParMng.getDbl(t, "Площадь.Общая");
                    rc.setTotalArea(BigDecimal.valueOf(totalArea));

                    // Транспортный GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    rc.setTransportGUID(t.getTguid());

                    ah.getNonResidentialPremiseToCreate().add(rc);
                }));

        // Обновить жилое помещение(ия)
        taskDao.getByTaskAddrTp(task, "Квартира", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_UPD_PRMS")) // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Обновление жилого помещения, Task.id={}, Guid={}", t.getId(), t.getEolink().getGuid());
                    ResidentialPremises rp = new ResidentialPremises();
                    ResidentialPremisesToUpdate rc = new ResidentialPremisesToUpdate();
                    // Тип - отдельная квартира
                    rc.setPremisesCharacteristic(ulistMng.getNsiElem("NSI", 30, "Характеристика помещения", "Отдельная квартира"));
                    Boolean isGkn = teParMng.getBool(t, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
                    if (isGkn != null && isGkn) {
                        // Ключ связи с ГКН/ЕГРП отсутствует.
                        rc.setNoRSOGKNEGRPRegistered(true);
                    } else {
                        // Ключ связи с ГКН/ЕГРП присутствует, поставить номер ГКН
                        String gknKey = teParMng.getStr(t, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                        if (gknKey == null) {
                            throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + t.getId());
                        }
                        rc.setCadastralNumber(gknKey);
                    }

                    // наличие подъезда
                    if (t.getEolink().getParent().getObjTp().getCd().equals("Подъезд")) {
                        // есть подъезд
                        // номер подъезда
                        String entryNum = String.valueOf(t.getEolink().getEntry());
                        if (entryNum != null) {
                            rc.setEntranceNum(entryNum);
                        }
                    } else {
                        // нет подъезда
                        rc.setHasNoEntrance(true);
                    }

                    // Номер помещения
                    rc.setPremisesNum(Utl.ltrim(t.getEolink().getKw(), "0"));
                    // Общая площадь
                    Double totalArea = teParMng.getDbl(t, "Площадь.Общая");
                    rc.setTotalArea(BigDecimal.valueOf(totalArea));
                    // Жилая площадь
                    Double grossArea = teParMng.getDbl(t, "Площадь.Жилая");
                    if (grossArea != null) {
                        rc.setGrossArea(BigDecimal.valueOf(grossArea));
                    } else {
                        rc.setNoGrossArea(true);
                    }
                    // Дата закрытия, если установлено - убрал параметр 26.12.2017 из за сложности восстановления через интерфейс ГИС!!!
	    	/*Date dtTerminate = teParMng.getDate(t, "ГИС ЖКХ.Дата закрытия");
	    	if (dtTerminate != null) {
		    	rc.setTerminationDate(Utl.getXMLDate(dtTerminate));
	    	}

	    	rc.setTerminationDate(null);*/

                    // Транспортный GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    rc.setTransportGUID(t.getTguid());

                    // GUID
                    rc.setPremisesGUID(t.getEolink().getGuid());

                    rp.setResidentialPremisesToUpdate(rc);
                    ah.getResidentialPremises().add(rp);
                }));

        // Обновить НЕжилое помещение(ия)
        taskDao.getByTaskAddrTp(task, "Помещение нежилое", null, 1).stream().filter(t -> t.getAct().getCd().equals("GIS_UPD_PRMS"))  // todo проверить задания - их нет нигде!
                .forEach(Errors.rethrow().wrap(t -> {
                    log.trace("Обновление НЕжилого помещения, Task.id={}, Guid={}", t.getId(), t.getEolink().getGuid());
                    NonResidentialPremiseToUpdate rc = new NonResidentialPremiseToUpdate();
                    Boolean isGkn = teParMng.getBool(t, "ГИС ЖКХ.Признак.ОтсутствияСвязи.ГКН");
                    if (isGkn != null && isGkn) {
                        // Ключ связи с ГКН/ЕГРП отсутствует.
                        rc.setNoRSOGKNEGRPRegistered(true);
                    } else {
                        // Ключ связи с ГКН/ЕГРП присутствует, поставить номер ГКН
                        String gknKey = teParMng.getStr(t, "ГИС ЖКХ.Кадастровый номер (для связывания сведений с ГКН и ЕГРП)");
                        if (gknKey == null) {
                            throw new CantPrepSoap("Отсутствует Кадастровый номер, для связывания сведений с ГКН и ЕГРП! Task.Id=" + t.getId());
                        }
                        rc.setCadastralNumber(gknKey);
                    }
                    // Номер помещения
                    rc.setPremisesNum(Utl.ltrim(t.getEolink().getKw(), "0"));
                    String commProp = teParMng.getStr(t, "Помещение, сост.общ.имущ.МКД");

                    rc.setIsCommonProperty(commProp.equals("Да"));

                    // Дата закрытия, если установлено - убрал параметр 26.12.2017 из за сложности восстановления через интерфейс ГИС!!!
	    	/*Date dtTerminate = teParMng.getDate(t, "ГИС ЖКХ.Дата закрытия");
	    	if (dtTerminate != null) {
		    	rc.setTerminationDate(Utl.getXMLDate(dtTerminate));
	    	}*/

                    // Общая площадь
                    Double totalArea = teParMng.getDbl(t, "Площадь.Общая");
                    rc.setTotalArea(BigDecimal.valueOf(totalArea));

                    // Транспортный GUID
                    String tguid = Utl.getRndUuid().toString();
                    t.setTguid(tguid);
                    rc.setTransportGUID(t.getTguid());

                    // GUID
                    rc.setPremisesGUID(t.getEolink().getGuid());
                    ah.getNonResidentialPremiseToUpdate().add(rc);
                }));

        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        req.setId("foo");
        req.setApartmentHouse(ah);
        AckRequest ack = null;

        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        try {
            ack = par.port.importHouseUOData(req);
        } catch (ru.gosuslugi.dom.schema.integration.house_management_service_async.Fault e1) {
            e1.printStackTrace();
            err = true;
            errMainStr = e1.getFaultInfo().getErrorMessage();
        }

        if (err) {
            task.setState("ERR");
            task.setResult("Ошибка при отправке XML: " + errMainStr);
            taskMng.logTask(task, false, false);

        } else {
            // Установить статус "Запрос статуса"
            task.setState("ACK");
            task.setMsgGuid(ack.getAck().getMessageGUID());
            taskMng.logTask(task, false, true);

        }
    }

    /**
     * Получить результат отправки обновления объектов дома
     */

    public void importHouseUODataAsk(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);

        // получить состояние
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
            return;
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
            //log.trace("Проверка1");
            Task foundTask2 = task;
            retState.getImportResult().forEach(t -> {
                //log.trace("Проверка2");
                t.getCommonResult().forEach(d -> {
                    //log.trace("Проверка3");

                    // Найти элемент задания по Транспортному GUID
                    Task task2 = taskMng.getByTguid(foundTask2, d.getTransportGUID());
                    if (d.getUpdateDate() != null) {
                        // Есть дата обновления, установить GUID
                        task2.setState("ACP");
                        task2.setGuid(d.getGUID());
                        task2.setUn(d.getUniqueNumber());
                        task2.setDtUpd(Utl.getDateFromXmlGregCal(d.getUpdateDate()));

                        // Переписать значения параметров в eolink из task
                        teParMng.acceptPar(task2);
                        // Установить статус активности
                        task2.getEolink().setStatus(1);

                        // Записать идентификаторы объекта, полученного от внешней системы (если уже не установлены)
                        taskMng.setEolinkIdf(task2.getEolink(), d.getGUID(), d.getUniqueNumber(), 1);
                        log.trace("После импорта объектов по Task.id={} и TGUID={}, получены следующие параметры:", task.getId(), d.getTransportGUID());
                        log.trace("GUID={}, UniqueNumber={}", d.getGUID(), d.getUniqueNumber());
                    }
                });
            });

            if (taskDao.getChildAnyErr(task)) {
                log.error("Найдены ошибки / Невыполнение в дочерних заданиях! Task.id={}", task.getId());
                // Установить статус
                task.setResult("Найдены ошибки / Невыполнение в дочерних заданиях!");
                task.setState("ERR");
                taskMng.logTask(task, false, false);

            } else {
                log.info("******* Task.id={}, Импорт объектов дома выполнен", task.getId());
                task.setState("ACP");
                taskMng.logTask(task, false, true);

            }
        }

    }


    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void createTasks(String createTaskCd, boolean isPrivate, String rptTaskCd) {
        for (HouseUkTaskRec t : eolinkDao2.getHouseByTpWoTaskTp(createTaskCd, isPrivate ? 1 : 0)) {

            Eolink eolHouse = em.find(Eolink.class, t.getEolHouseId());
            Eolink procUk = em.find(Eolink.class, t.getEolUkId());
            Task newTask4 = ptb.setUp(eolHouse, null, null, createTaskCd, "ACP", config.getCurUserGis().get().getId(), procUk);
            ptb.save(newTask4);
            log.info("Добавлено задание CD={}, по Дому Eolink.id={}, Task.procUk.id={}", createTaskCd, eolHouse.getId(), procUk.getId());
            // добавить зависимое задание к системному повторяемому заданию
            // (будет запускаться системным заданием)
            ptb.addAsChild(newTask4, rptTaskCd);

        }
    }

    @Transactional(propagation = Propagation.REQUIRED, rollbackFor = Exception.class)
    public void createTasks(String parentCd, String createTaskCd, String rptTaskCd) {
        for (HouseUkTaskRec t : eolinkDao2.getHouseByTpWoTaskTp(parentCd, createTaskCd, 0)) {

            Eolink eolHouse = em.find(Eolink.class, t.getEolHouseId());
            Eolink procUk = em.find(Eolink.class, t.getEolUkId());
            Task masterTask = em.find(Task.class, t.getMasterTaskId());
            Task newTask3 = ptb.setUp(eolHouse, null, masterTask, createTaskCd, "ACP", config.getCurUserGis().get().getId(), procUk);
            ptb.save(newTask3);
            log.info("Добавлено задание CD={}, по Дому Eolink.id={}, Task.procUk.id={}", createTaskCd, eolHouse.getId(), procUk.getId());
            // добавить зависимое задание к системному повторяемому заданию
            // (будет запускаться системным заданием)
            ptb.addAsChild(newTask3, rptTaskCd);

        }
    }


    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void createSetOfTasks(Eolink eolHouse, String rptTaskCd) throws WrongParam {
        Task parent = createParentTask(eolHouse, rptTaskCd, "GIS_EXP_HOUSE");
        log.info("Добавлено задание CD={}, по Дому Eolink.id={}", "GIS_EXP_HOUSE", eolHouse.getId());
        // сохранить ведущее задание

        // создать зависимое задание, выгрузки счетчиков ИПУ. оно не должно запуститься до выполнения ведущего GIS_EXP_HOUSE
        Task newTask2 = ptb.setUp(eolHouse, null, parent, "GIS_EXP_METERS", "ACP", config.getCurUserGis().get().getId(), null);
        // добавить как зависимое задание к системному повторяемому заданию
        ptb.addTaskPar(newTask2, "ГИС ЖКХ.Включая архивные", null, null, false, null);
        ptb.addAsChild(newTask2, rptTaskCd);
        ptb.save(newTask2);
        log.info("Добавлено задание CD={}, по Дому Eolink.id={}", "GIS_EXP_METERS", eolHouse.getId());
    }

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public Task createParentTask(Eolink eolHouse, String rptTaskCd, String taskCd) {
        Task parent = ptb.setUp(eolHouse, null, taskCd, "ACP", config.getCurUserGis().get().getId());
        // добавить как зависимое задание к системному повторяемому заданию
        ptb.addAsChild(parent, rptTaskCd);
        ptb.save(parent);
        return parent;
    }

}
