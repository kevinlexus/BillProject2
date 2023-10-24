package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.EolinkMng;
import com.dic.app.gis.service.maintaners.EolinkParMng;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.maintaners.impl.UlistMng;
import com.dic.app.gis.service.soap.SoapConfigs;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.service.ConfigApp;
import com.dic.bill.dao.*;
import com.dic.bill.dto.MeterData;
import com.dic.bill.dto.MeterValue;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.scott.ObjPar;
import com.dic.bill.model.scott.Tuser;
import com.ric.cmn.CommonErrs;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.AllArgsConstructor;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;
import ru.gosuslugi.dom.schema.integration.base.AckRequest;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType;
import ru.gosuslugi.dom.schema.integration.base.CommonResultType.Error;
import ru.gosuslugi.dom.schema.integration.base.GetStateRequest;
import ru.gosuslugi.dom.schema.integration.device_metering.*;
import ru.gosuslugi.dom.schema.integration.device_metering.ImportMeteringDeviceValuesRequest.MeteringDevicesValues;
import ru.gosuslugi.dom.schema.integration.device_metering.ImportMeteringDeviceValuesRequest.MeteringDevicesValues.ElectricDeviceValue;
import ru.gosuslugi.dom.schema.integration.device_metering.ImportMeteringDeviceValuesRequest.MeteringDevicesValues.OneRateDeviceValue;
import ru.gosuslugi.dom.schema.integration.device_metering_service_async.DeviceMeteringPortTypesAsync;
import ru.gosuslugi.dom.schema.integration.device_metering_service_async.DeviceMeteringServiceAsync;
import ru.gosuslugi.dom.schema.integration.device_metering_service_async.Fault;
import ru.gosuslugi.dom.schema.integration.nsi_base.NsiRef;

import javax.persistence.EntityManager;
import javax.persistence.ParameterMode;
import javax.persistence.StoredProcedureQuery;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.ws.BindingProvider;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class DeviceMeteringAsyncBindingBuilder {

    public static final String USER_GIS_CD = "GIS";
    private static final Integer STATUS_CREATED = 0; // 0-добавлен на загрузку в ГИС
    private static final Integer STATUS_PROCESS = 1; // 1-в процессе загрузки в ГИС
    private static final Integer STATUS_LOADED = 2;  // 2-загружен в ГИС
    public static final Integer STATUS_LOADED_FROM_GIS = 3; // 3-принят из ГИС
    public static final Integer STATUS_ERROR_WHILE_LOAD_TO_GIS = 4; // 4-ошибка при загрузке в ГИС (например счетчик был архивирован, и пытаются передать показания)
    private final EntityManager em;
    private final UlistMng ulistMng;
    private final TaskMng taskMng;
    private final EolinkDAO2 eolinkDAO2;
    private final SoapConfigs soapConfig;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;
    private final MeterDAO meterDao;
    private final MeterMng meterMng;
    private final EolinkMng eolinkMng;
    private final TuserDAO tuserDAO;
    private final ObjParDAO objParDAO;


    @AllArgsConstructor
    static class SoapPar {
        private DeviceMeteringPortTypesAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
    }

    @AllArgsConstructor
    private class Result {
        GetStateResult stateResult;
        List<String> errorTguids;
    }

    /**
     * Инициализация - создать сервис и порт
     */
    private SoapPar setUp(Task task) throws CantSendSoap, CantPrepSoap {
        DeviceMeteringServiceAsync service = new DeviceMeteringServiceAsync();
        DeviceMeteringPortTypesAsync port = service.getDeviceMeteringPortAsync();

        // подоготовительный объект для SOAP
        SoapBuilder sb = new SoapBuilder();
        ReqProp reqProp = new ReqProp(config, task, eolParMng);
        sb.setUp((BindingProvider) port, true, reqProp.getPpGuid(),
                reqProp.getHostIp(), true);

        // логгинг запросов
        sb.setTrace(task.getTrace().equals(1));
        // Id XML подписчика
        sb.setSignerId(reqProp.getSignerId());
        return new SoapPar(port, sb, reqProp);
    }


    /**
     * Получить состояние запроса
     *
     * @param task задание
     */
    private Result getState2(Task task) throws CantPrepSoap, CantSendSoap {
        // Признак ошибки
        boolean err = false;
        // Признак ошибки в CommonResult
        boolean errChld = false;
        String errStr = null;
        ru.gosuslugi.dom.schema.integration.device_metering.GetStateResult state = null;

        GetStateRequest gs = new GetStateRequest();
        gs.setMessageGUID(task.getMsgGuid());
        SoapPar par = setUp(task);
        par.sb.setSign(false); // не подписывать запрос состояния!

        String errMsg = null;
        List<String> errorTguids = new ArrayList<>(); // tguid показаний, которые были переданы с ошибкой
        try {
            state = par.port.getState(gs);
        } catch (Fault e) {
            errMsg = e.getFaultInfo().getErrorCode();
            e.printStackTrace();
            err = true;
            errStr = "Запрос вернул ошибку!";
        }

        if (state != null && state.getRequestState() != 3) {
            // вернуться, если задание всё еще не выполнено
            log.trace("Статус запроса={}, Task.id={}", state.getRequestState(), task.getId());
            if (state.getRequestState() == 1) {
                // статус запроса - ACK - увеличить время ожидания + 10 секунд
                taskMng.alterDtNextStart(task);
            }
            return null;
        }

        // Показать ошибки, если есть
        // не ситуация, когда экспорт счетчиков и ошибка "Нет объектов для экспорта"
        if (err) {
            // Ошибки во время выполнения
            log.trace(errStr);
            task.setState("ERR");
            task.setResult(errMsg);
            log.error("Task.id={}, ОШИБКА выполнения запроса = {}", task.getId(), errStr);
        } else if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null
                && !(task.getAct().getCd().equals("GIS_EXP_METER_VALS") && state.getErrorMessage().getErrorCode().equals("INT002012"))
        ) {
            // Ошибки контролей или бизнес-процесса
            err = true;
            errStr = state.getErrorMessage().getDescription();
            log.trace("Ошибка выполнения запроса errStr={}, errCode={}", errStr, state.getErrorMessage().getErrorCode());
            task.setState("ERR");
            task.setResult(errStr);
        } else {

            for (CommonResultType e : state.getImportResult()) {
                for (Error f : e.getError()) {
                    log.error("Error code={}, Description={}, TransportGUID={}",
                            f.getErrorCode(), f.getDescription(), e.getTransportGUID());
                    errorTguids.add(e.getTransportGUID());
                    errChld = true;
                }
            }
        }

        if (!err) {
            // если в главном задании нет ошибок, но в любом дочернем задании обнаружена ошибка - статус - "Ошибка"
            // и если уже не установлен признак ошибки
            if (errChld && !task.getState().equals("ERR")
                    && !task.getState().equals("ERS")) {
                task.setState("ERS");
                task.setResult(errStr);
                log.error("Ошибки в элементе CommonResult");
            }
        }

        return new Result(state, errorTguids);
    }

    /**
     * Импортировать показания счетчиков
     *
     * @param taskId Id задания
     */

    public void importMeteringDeviceValues(Integer taskId) throws DatatypeConfigurationException, CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        SoapPar par = setUp(task);
        AckRequest ack = null;
        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ImportMeteringDeviceValuesRequest req = new ImportMeteringDeviceValuesRequest();

        req.setId("foo");
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        req.setFIASHouseGuid(par.reqProp.getHouseGuid());

        Eolink house = task.getEolink();
        List<Integer> statuses = List.of(STATUS_CREATED, STATUS_PROCESS); // 0-добавлен на загрузку в ГИС, 1-в процессе загрузки в ГИС
        List<MeterValue> meterValues = meterDao.getHouseMeterValue(house.getGuid(), config.getPeriod(), statuses).stream()
                .limit(900) // гис больше 1000 не принимает
                .collect(Collectors.toList());
        if (meterValues.size()==0) {
            log.info("Нет показаний счетчиков на отправку");
            task.setState("ACP");
            taskMng.logTask(task, false, true);
            return;
        }
        for (MeterValue meterValue : meterValues) {

            ObjPar objPar = em.find(ObjPar.class, meterValue.getId());
            String tguid = Utl.getRndUuid().toString();
            objPar.setTguid(tguid);
            objPar.setStatus(STATUS_PROCESS); // статус 1-в процессе загрузки в ГИС

            // счетчик физический (корневой)
            Eolink meter = em.find(Eolink.class, meterValue.getEolinkId());
            MeteringDevicesValues val = new MeteringDevicesValues();
            val.setMeteringDeviceRootGUID(meter.getGuid());

            log.info("Найден счетчик, eolink.id={}, usl={}", meter.getId(), meter.getUsl());
            if (ulistMng.getResType(meter.getUsl()) == 1) {
                ElectricDeviceValue elVal = new ElectricDeviceValue();
                ElectricDeviceValue.CurrentValue currElVal = new ElectricDeviceValue.CurrentValue();

                // Дата снятия показания
                currElVal.setDateValue(Utl.getXMLDate(meterValue.getDtCrt()));

                // показания по тарифам
                currElVal.setMeteringValueT1(meterValue.getN1().toString());

                currElVal.setTransportGUID(tguid);
                elVal.setCurrentValue(currElVal);
                // эл.энерг.
                val.setElectricDeviceValue(elVal);
                log.info("Отправлены в ГИС: Эл.Эн. счетчик, eolink.id={}, usl={}, показание={}", meter.getId(), meter.getUsl(), meterValue.getN1());
            } else if (ulistMng.getResType(meter.getUsl()) == 0) {
                OneRateDeviceValue oneRateVal = new OneRateDeviceValue();
                OneRateDeviceValue.CurrentValue currOneRateVal = new OneRateDeviceValue.CurrentValue();
                currOneRateVal.setDateValue(Utl.getXMLDate(meterValue.getDtCrt()));

                // показания по тарифам
                currOneRateVal.setMeteringValue(meterValue.getN1().toString());

                // Получить ресурс по коду USL
                NsiRef mres = ulistMng.getResourceByUsl(meter.getUsl());

                currOneRateVal.setMunicipalResource(mres);
                currOneRateVal.setTransportGUID(tguid);
                oneRateVal.getCurrentValue().add(currOneRateVal);
                // г.в., х.в.
                val.setOneRateDeviceValue(oneRateVal);
            }
            log.info("Отправлены в ГИС: счетчик ресурса, eolink.id={}, usl={}, показание={}", meter.getId(), meter.getUsl(), meterValue.getN1());
            req.getMeteringDevicesValues().add(val);
        }

        try {
            ack = par.port.importMeteringDeviceValues(req);
        } catch (Fault e) {
            e.printStackTrace();
            err = true;
            errMainStr = e.getFaultInfo().getErrorMessage();
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
     * Получить результат импорта показаний счетиков
     *
     * @param taskId Id задания
     */

    public void importMeteringDeviceValuesAsk(Integer taskId) throws CantPrepSoap, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        setUp(task);
        // получить состояние
        Result result = getState2(task);

        if (result != null) {
            GetStateResult retState = result.stateResult;
            if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
                // обработать ошибки
                for (String tguid : result.errorTguids) {
                    log.error("Показания, переданные с ошибкой TGUID={}", tguid);
                    ObjPar objpar = objParDAO.findByTguid(tguid);
                    objpar.setStatus(STATUS_ERROR_WHILE_LOAD_TO_GIS);
                }

                // остальные, корректно загруженные
                retState.getImportResult().forEach(t -> {
                    if (!result.errorTguids.contains(t.getTransportGUID())) {
                        log.trace("После импорта объектов по Task.id={} и TGUID={}, получены следующие параметры:",
                                task.getId(), t.getTransportGUID());
                        log.trace("UniqueNumber={}, Дата обновления={}", t.getUniqueNumber(), Utl.getDateFromXmlGregCal(t.getUpdateDate()));
                        ObjPar objpar = objParDAO.findByTguid(t.getTransportGUID());
                        objpar.setStatus(STATUS_LOADED);
                    }
                });

                // Установить статус выполнения задания
                task.setState("ACP");
                taskMng.logTask(task, false, true);

            }
        }
    }

    /**
     * Экспортировать показания счетчиков по дому
     *
     * @param taskId Id задания
     */

    public void exportMeteringDeviceValues(Integer taskId) throws CantPrepSoap, WrongGetMethod, DatatypeConfigurationException, WrongParam, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);
        // Установить параметры SOAP
        SoapPar par = setUp(task);

        // РКЦ (с параметрами)
        Eolink rkc = soapConfig.getRkcByHouse(task.getEolink());

        AckRequest ack = null;
        // для обработки ошибок
        boolean err = false;
        String errMainStr = null;

        ExportMeteringDeviceHistoryRequest req = new ExportMeteringDeviceHistoryRequest();

        req.setId("foo");
        // требование гис передать именно так версию
        req.setVersion("10.0.1.1");
        //req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
        req.getFIASHouseGuid().add(par.reqProp.getHouseGuid());

        // индивидуальные приборы учета
        NsiRef tp = ulistMng.getNsiElem("NSI", 27, "Тип прибора учета", "Индивидуальный");
        req.getMeteringDeviceType().add(tp);

        // опции проверки
/*		Boolean checkOneOpt=false;
        Boolean checkTwoOpt=false;
		// Добавить параметры фильтрации показаний
		не удалять закомментированное, в будущем может пригодиться! ред. 27.07.2018
		for (TaskPar p :task.getTaskPar()) {

			// Фильтр - Тип - виды приборов учета
			if (p.getPar().getCd().equals("Счетчик.Тип")) {
				checkOneOpt=true;
				log.trace("Тип прибора учета1={}", p.getS1());
				NsiRef tp = ulistMng.getNsiElem("NSI", 27, "Тип прибора учета", p.getS1());
				req.getMeteringDeviceType().add(tp);
			}
			// Фильтр - Вид коммунального ресурса
			if (p.getPar().getCd().equals("Счетчик.ВидКоммунРесурса")) {
				if (checkOneOpt) {
					throw new CantPrepSoap("Некорректное количество критериев запроса!");
				}
				checkTwoOpt=true;
				//log.trace("Вид коммун ресурса1={}", p.getS1());
				NsiRef tp = ulistMng.getNsiElem("NSI", 2, "Вид коммунального ресурса", p.getS1());
				//log.trace("Вид коммун ресурса2={}", tp.getName());
				req.getMunicipalResource().add(tp);
			}
		}
*/

        // включать ли архивные счетчики
        req.setSerchArchived(false);
        // исключать показания отправленные информационной системой
        req.setExcludeISValues(true);

        // дата с которой получить показания
        // использовать период экспорта извещений (должна совпадать)
        String period = eolParMng.getStr(rkc, "ГИС ЖКХ.PERIOD_EXP_NOTIF");
        try {
            req.setInputDateFrom(Utl.getXMLDate(Utl.getDateFromPeriod(period)));
        } catch (ParseException e) {
            log.error(Utl.getStackTraceString(e));
            throw new WrongParam("ERROR! Некорректный период");
        }

        try {
            ack = par.port.exportMeteringDeviceHistory(req);
        } catch (Fault e) {
            e.printStackTrace();
            err = true;
            errMainStr = e.getFaultInfo().getErrorMessage();
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
     * Получить результат экспорта показаний счетиков
     *
     * @param taskId Id задания
     */

    @CacheEvict(value = {"EolinkDAOImpl.getEolinkByGuid"}, allEntries = true) // здесь Evict потому что
    // пользователь может обновить Ko объекта счетчика из Директа(осуществить привязку)
    // и тогда должен быть получен обновленный объект! ред.07.12.18
    public void exportMeteringDeviceValuesAsk(Integer taskId) throws WrongGetMethod, CantPrepSoap, WrongParam, UnusableCode, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        eolinkMng.lock(task.getEolink().getId());

        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние запроса
        Result result = getState2(task);

        if (result == null) {
            // не обработано
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
            GetStateResult retState = result.stateResult;
            // РКЦ (с параметрами)
            Eolink rkc = soapConfig.getRkcByHouse(task.getEolink());
            // дом
            Eolink house = task.getEolink();
            // погасить по дому сообщение об ошибке
            soapConfig.saveError(house, CommonErrs.ERR_METER_NOT_FOUND_BY_GUID, false);
            // получить периоды выгрузки
            String period = eolParMng.getStr(rkc, "ГИС ЖКХ.PERIOD_EXP_NOTIF");
            Date dt1;
            try {
                dt1 = Utl.getDateFromPeriod(period);
            } catch (ParseException e) {
                log.error(Utl.getStackTraceString(e));
                throw new WrongParam("ERROR! Некорректный период");
            }
            Date dt2 = Utl.getLastDate(dt1);

            // получить уже сохранённые в базу показания
            List<MeterData> mdLst = meterDao.findMeteringDataTsUsingUser(USER_GIS_CD, "ins_sch", period);
            Map<String, Date> existVal = new HashMap<>();
            mdLst.forEach(t-> existVal.put(t.getGuid(), t.getTs()));
            for (ExportMeteringDeviceHistoryResultType t : retState.getExportMeteringDeviceHistoryResult()) {
                // найти счетчик по GUID
                Eolink meterEol = eolinkDAO2.findEolinkByGuid(t.getMeteringDeviceRootGUID());
                if (meterEol == null) {
                    // счетчик не найден, записать по дому сообщение об ошибке
                    soapConfig.saveError(house, CommonErrs.ERR_METER_NOT_FOUND_BY_GUID, true);
                    log.trace("При выгрузке показаний, счетчик с GUID={} НЕ НАЙДЕН, ожидается его экспорт из ГИС",
                            t.getMeteringDeviceRootGUID());
                } else {
                    if (meterEol.getKoObj() != null) {
                        if (meterMng.getCanSaveDataMeter(meterEol, dt2)) {
                            // погасить ошибку неактуальности счетчика в Директ
                            soapConfig.saveError(meterEol, CommonErrs.ERR_METER_NOT_ACTUAL_DIRECT, false);
                            // погасить ошибку отсутствия связи со счетчиком в Директ
                            soapConfig.saveError(meterEol, CommonErrs.ERR_METER_NOT_ASSOC_DIRECT, false);
                            if (t.getOneRateDeviceValue() != null) {
                                for (OneRateCurrentMeteringValueExportType e :
                                        t.getOneRateDeviceValue().getValues().getCurrentValue()) {
                                    // проверить сохранены ли уже показания
                                    if (Utl.between(Utl.getDateFromXmlGregCal(e.getEnterIntoSystem()), dt1, dt2) &&
                                            !meterMng.getIsMeterDataExist(existVal, t.getMeteringDeviceRootGUID(),
                                                    e.getEnterIntoSystem()) && e.getMeteringValue() != null) {
                                        // показания еще не были сохранены, сохранить
                                        log.info("Получены показания по OneRateDeviceValue: " +
                                                        "MeteringDeviceRootGUID={} DateValue={}, EnterIntoSystem={}, OrgPPAGUID={}, " +
                                                        "ReadingSource={}, val={}",
                                                t.getMeteringDeviceRootGUID(), e.getDateValue(),
                                                e.getEnterIntoSystem(), e.getOrgPPAGUID(), e.getReadingsSource(),
                                                e.getMeteringValue());
                                        // сохранить показание по счетчику в базу
                                        saveMeterData(meterEol, e.getMeteringValue(), e.getEnterIntoSystem(), STATUS_LOADED_FROM_GIS);
                                    }
                                }
                            }
                            if (t.getElectricDeviceValue() != null) {
                                for (ElectricCurrentMeteringValueExportType e :
                                        t.getElectricDeviceValue().getValues().getCurrentValue()) {
                                    // проверить сохранены ли уже показания
                                    if (Utl.between(Utl.getDateFromXmlGregCal(e.getEnterIntoSystem()), dt1, dt2) &&
                                            !meterMng.getIsMeterDataExist(existVal, t.getMeteringDeviceRootGUID(),
                                                    e.getEnterIntoSystem()) && e.getMeteringValueT1() != null) {
                                        log.trace("показания по ElectricDeviceValue: GUID={} date={}, enter={}, val={}",
                                                t.getMeteringDeviceRootGUID(), e.getDateValue(), e.getEnterIntoSystem(),
                                                e.getMeteringValueT1());
                                        // сохранить показание по счетчику в базу
                                        saveMeterData(meterEol, e.getMeteringValueT1(), e.getEnterIntoSystem(), STATUS_LOADED_FROM_GIS);
                                    }
                                }

                            }
                        } else {
                            soapConfig.saveError(meterEol, CommonErrs.ERR_METER_NOT_ACTUAL_DIRECT, true);
                            log.error("Счетчик Eolink.id={} не является актуальным или отключена связь в Директ ", meterEol.getId());
                        }
                    } else {
                        // Ko не заполнен (счетчик не привязан к Директ, для выгрузки)
                        soapConfig.saveError(meterEol, CommonErrs.ERR_METER_NOT_ASSOC_DIRECT, true);
                        log.error("Счетчик Eolink.id={} не привязан к соответствующему счетчику в Директ " +
                                "по Eolink.FK_KLSK_OBJ", meterEol.getId());
                    }

                }
            }
            // Установить статус выполнения задания
            task.setState("ACP");
            log.info("******* ACP");
            taskMng.logTask(task, false, true);
        }
    }

    /**
     * Сохранение показания по счетчику в базу
     *
     * @param meter  счетчик
     * @param num1   показание
     * @param ts     timestamp
     * @param status Id статуса
     */
    public void saveMeterData(Eolink meter, String num1, XMLGregorianCalendar ts, Integer status) throws UnusableCode, WrongParam {
        // усечь до секунд
        Date dt = Utl.truncDateToSeconds(Utl.getDateFromXmlGregCal(ts));
        // погасить ошибку записи в базу
        soapConfig.saveError(meter, CommonErrs.ERROR_WHILE_SAVING_DATA, false);
        Tuser user = tuserDAO.getByCd(USER_GIS_CD);
        if (user == null)
            throw new WrongParam(String.format("Не найден пользователь для ГИС: scott.t_user.cd='%s'", USER_GIS_CD));

        // default параметры тоже должны быть перечислены!
        StoredProcedureQuery qr = em.createStoredProcedureQuery("scott.p_meter.ins_data_meter");
        qr.registerStoredProcedureParameter("p_met_klsk", Long.class, ParameterMode.IN); // p_met_klsk
        qr.registerStoredProcedureParameter("p_n1", Double.class, ParameterMode.IN); // p_n1
        qr.registerStoredProcedureParameter("p_ts", Date.class, ParameterMode.IN); // p_ts
        qr.registerStoredProcedureParameter("p_status", Integer.class, ParameterMode.IN); // p_status
        qr.registerStoredProcedureParameter("p_user", Integer.class, ParameterMode.IN); // p_user
        qr.registerStoredProcedureParameter("p_ret", Long.class, ParameterMode.OUT);// p_ret

        qr.setParameter("p_met_klsk", meter.getKoObj().getId());
        qr.setParameter("p_n1", Double.valueOf(num1));
        qr.setParameter("p_ts", dt);
        qr.setParameter("p_status", status);
        qr.setParameter("p_user", user.getId());
        qr.execute();
        Long ret = (Long) qr.getOutputParameterValue("p_ret");
        if (!ret.equals(0L)) {
            soapConfig.saveError(meter, CommonErrs.ERROR_WHILE_SAVING_DATA, true);
        }
        log.trace("Результат исполнения scott.p_meter.ins_data_meter={}", ret);
    }

    /**
     * Записать показания по счетчикам в файл
     *
     * @param taskId Id задания
     */

    public void saveValToFile(Integer taskId) throws WrongGetMethod, IOException {
        Task task = em.find(Task.class, taskId);
    /*log.trace("******* Task.id={}, Выгрузка показаний приборов учета в файл path={}", task.getId(), appTp, pathCounter);
        Task foundTask = em.find(Task.class, task.getId());
        if (appTp.equals("0")) {
            File file = new File(pathCounter);
            FileWriter fw = null;
            BufferedWriter bw = null;
            boolean fileLinked = false;

            for (Eolink meter : eolinkDao.getValsNotSaved()) {
                // ЗАПИСАТЬ показания во внешний файл
                Integer cLskId = null;
                // найти первый попавшийся лицевой счет (по словам Андрейки ред. 22.09.18)
                for (Eolink t : meter.getParentLinked()) {
                    cLskId = t.getCLskId();
                    break;
                }
                Integer tp = null;
                switch (meter.getUsl()) {
                    case "011":
                        tp = 1;
                        break;
                    case "015":
                        tp = 2;
                        break;
                    case "024":
                        tp = 3;
                        break;
                }
                if (cLskId == null) {
                    log.error("К счетчику не привязан лицевой счет, Eolink.id={}", meter.getId());
                } else if (tp != null) {
                    // предыдущее показание
                    Double prevVal = eolinkParMng.getDbl(meter, "Счетчик.ПоказПредыдущее(Т1)");
                    // текущее показание
                    Double val = eolinkParMng.getDbl(meter, "Счетчик.Показ(Т1)");
                    // объем = новое показание - предыдущее показание
                    Double vol = Utl.nvl(val, 0D) - Utl.nvl(prevVal, 0D);
                    String log_id = "";
                    if (val != null) {
                        try {
                            if (!fileLinked) {
                                if (!file.exists()) file.createNewFile();
                                fw = new FileWriter(file.getAbsoluteFile());
                                bw = new BufferedWriter(fw);
                                fileLinked = true;
                            }
                            log.trace("Показания по Eolink.id={}", meter.getId());
                            log_id = em.createNativeQuery("Select exs.seq_log.nextval from dual").getSingleResult().toString();
                            String str = log_id + "|" + cLskId + "|" + tp + "|" + eolinkParMng.getDbl(meter, "Счетчик.Показ(Т1)") + "|" + vol + "|" +
                                    Utl.getStrFromDate(new Date(), "dd.MM.yyyy HH:mm:ss") + "|" + Utl.nvl(meter.getIdGrp(), "0") + "|" + Utl.nvl(meter.getIdCnt(), "0");
                            log.trace(str);
                            bw.write(str);
                            bw.newLine();
                        } catch (IOException e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        } catch (WrongGetMethod e) {
                            // TODO Auto-generated catch block
                            e.printStackTrace();
                        }
                    }
                    // пометить сохранённым в файл
                    eolinkParMng.setDbl(meter, "ГИС ЖКХ.Счетчик.СтатусОбработкиПоказания", 0D);
                }
            }
            if (bw != null)
                bw.close();

            if (fw != null)
                fw.close();
        }
        // Установить статус выполнения задания
        foundTask.setState("ACP"); */
    }


}
