package com.dic.app.gis.service.soapbuilders.impl;


import com.dic.app.gis.service.maintaners.*;
import com.dic.app.gis.service.maintaners.impl.ReqProp;
import com.dic.app.gis.service.maintaners.impl.UlistMng;
import com.dic.app.gis.service.soap.SoapConfigs;
import com.dic.app.gis.service.soap.impl.SoapBuilder;
import com.dic.app.mm.ConfigApp;
import com.dic.bill.UlistDAO;
import com.dic.bill.dao.EolinkDAO;
import com.dic.bill.dao.MeterDAO;
import com.dic.bill.dao.TaskDAO;
import com.dic.bill.dto.MeterData;
import com.dic.bill.mm.MeterMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.MeterVal;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.Ulist;
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
import java.math.BigDecimal;
import java.text.ParseException;
import java.util.Date;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
@Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
@RequiredArgsConstructor
public class DeviceMeteringAsyncBindingBuilder {

    private final EntityManager em;
    private final UlistMng ulistMng;
    private final TaskParMng taskParMng;
    private final TaskMng taskMng;
    private final TaskDAO taskDao;
    private final EolinkDAO eolinkDao;
    private final UlistDAO ulistDao;
    private final TaskEolinkParMng teParMng;
    private final SoapConfigs soapConfig;
    private final ConfigApp config;
    private final EolinkParMng eolParMng;
    private final MeterDAO meterDao;
    private final MeterMng meterMng;
    private final PseudoTaskBuilder ptb;

    @AllArgsConstructor
    static class SoapPar {
        private DeviceMeteringPortTypesAsync port;
        private SoapBuilder sb;
        private ReqProp reqProp;
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
    private GetStateResult getState2(Task task) throws CantPrepSoap, CantSendSoap {
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
        } else if (state.getErrorMessage() != null && state.getErrorMessage().getErrorCode() != null && !(task.getAct().getCd().equals("GIS_EXP_METER_VALS") && state.getErrorMessage().getErrorCode().equals("INT002012"))
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
                    // Найти элемент задания по Транспортному GUID
                    Task task2 = taskMng.getByTguid(task, e.getTransportGUID());
                    // Установить статусы ошибки по заданиям
                    task2.setState("ERR");
                    errStr = String.format("Error code=%s, Description=%s", f.getErrorCode(), f.getDescription());
                    task2.setResult(errStr);
                    log.error(errStr);

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

        return state;

    }

    /**
     * Импортировать показания счетчиков
     *
     * @param taskId Id задания
     */

    public Boolean importMeteringDeviceValues(Integer taskId) throws WrongGetMethod, DatatypeConfigurationException, CantPrepSoap, CantSendSoap {
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

        List<Task> lstTask = taskDao.getByTaskAddrTp(task, "СчетчикФизический", null, 1).stream()
                .filter(t -> t.getAct().getCd().equals("GIS_ADD_METER_VAL")).collect(Collectors.toList());
        for (Task t : lstTask) {
            // Транспортный GUID
            String tguid = Utl.getRndUuid().toString();
            t.setTguid(tguid);

            // счетчик физический (корневой)
            Eolink meter = t.getEolink();

            Date dtGet = taskParMng.getDate(t, "Счетчик.ДатаСнятияПоказания");
            MeteringDevicesValues val = new MeteringDevicesValues();
            val.setMeteringDeviceRootGUID(meter.getGuid());
            //val.setMeteringDeviceVersionGUID(meterVers.getGuid());

            if (ulistMng.getResType(meter.getUsl()) == 1) {
                ElectricDeviceValue elVal = new ElectricDeviceValue();
                ElectricDeviceValue.CurrentValue currElVal = new ElectricDeviceValue.CurrentValue();

                // Дата снятия показания
                currElVal.setDateValue(Utl.getXMLDate(dtGet));

                // показания по тарифам
                Double metVal = taskParMng.getDbl(t, "Счетчик.Показ(Т1)");
                currElVal.setMeteringValueT1(String.valueOf(metVal));
                metVal = taskParMng.getDbl(t, "Счетчик.Показ(Т2)");
                if (metVal != null) {
                    currElVal.setMeteringValueT2(String.valueOf(metVal));
                }
                metVal = taskParMng.getDbl(t, "Счетчик.Показ(Т3)");
                if (metVal != null) {
                    currElVal.setMeteringValueT3(String.valueOf(metVal));
                }

                currElVal.setTransportGUID(tguid);
                elVal.setCurrentValue(currElVal);
                // эл.энерг.
                val.setElectricDeviceValue(elVal);
            } else if (ulistMng.getResType(meter.getUsl()) == 0) {
                OneRateDeviceValue oneRateVal = new OneRateDeviceValue();
                OneRateDeviceValue.CurrentValue currOneRateVal = new OneRateDeviceValue.CurrentValue();
                currOneRateVal.setDateValue(Utl.getXMLDate(dtGet));

                // показания по тарифам
                Double metVal = taskParMng.getDbl(t, "Счетчик.Показ(Т1)");
                currOneRateVal.setMeteringValue(String.valueOf(metVal));

                // Получить ресурс по коду USL
                NsiRef mres = ulistMng.getResourceByUsl(meter.getUsl());

                currOneRateVal.setMunicipalResource(mres);
                currOneRateVal.setTransportGUID(tguid);
                oneRateVal.getCurrentValue().add(currOneRateVal);
                // г.в., х.в.
                val.setOneRateDeviceValue(oneRateVal);
            }
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
        return err;

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
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
            return;
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
            retState.getImportResult().stream().forEach(t -> {
                log.trace("После импорта объектов по Task.id={} и TGUID={}, получены следующие параметры:",
                        task.getId(), t.getTransportGUID());
                log.trace("UniqueNumber={}, Дата обновления={}", t.getUniqueNumber(), Utl.getDateFromXmlGregCal(t.getUpdateDate()));
                // Найти элемент задания по Транспортному GUID
                Task task2 = taskMng.getByTguid(task, t.getTransportGUID());
                // Переписать значения параметров в eolink из task
                teParMng.acceptPar(task2);
                task2.setState("ACP");

            });

            // Установить статус выполнения задания
            task.setState("ACP");
            taskMng.logTask(task, false, true);


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
        req.setVersion(req.getVersion() == null ? par.reqProp.getGisVersion() : req.getVersion());
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
        req.setExcludeISValues(false);

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
    // пользователь может обновить Ko объекта счетчика мз Директа(осуществить привязку)
    // и тогда должен быть получен обновленный объект! ред.07.12.18
    public void exportMeteringDeviceValuesAsk(Integer taskId) throws WrongGetMethod, CantPrepSoap, WrongParam, UnusableCode, CantSendSoap {
        Task task = em.find(Task.class, taskId);
        taskMng.logTask(task, true, null);

        // Установить параметры SOAP
        setUp(task);

        // получить состояние запроса
        GetStateResult retState = getState2(task);

        if (retState == null) {
            // не обработано
        } else if (!task.getState().equals("ERR") && !task.getState().equals("ERS")) {
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
            List<MeterData> mdLst = meterDao.findMeteringDataTsUsingUser("GIS", "ins_sch", period);
            for (ExportMeteringDeviceHistoryResultType t : retState.getExportMeteringDeviceHistoryResult()) {
                // найти счетчик по GUID
                Eolink meterEol = eolinkDao.getEolinkByGuid(t.getMeteringDeviceRootGUID());
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
                                            !meterMng.getIsMeterDataExist(mdLst, t.getMeteringDeviceRootGUID(),
                                                    e.getEnterIntoSystem()) && e.getMeteringValue() != null) {
                                        // показания еще не были сохранены, сохранить
                                        log.trace("Получены показания по OneRateDeviceValue: " +
                                                        "MeteringDeviceRootGUID={} DateValue={}, EnterIntoSystem={}, OrgPPAGUID={}, " +
                                                        "ReadingSource={}, val={}",
                                                t.getMeteringDeviceRootGUID(), e.getDateValue(),
                                                e.getEnterIntoSystem(), e.getOrgPPAGUID(), e.getReadingsSource(),
                                                e.getMeteringValue());
                                        // сохранить показание по счетчику в базу
                                        saveMeterData(meterEol, e.getMeteringValue(), e.getEnterIntoSystem(), period);
                                    }
                                }
                            }
                            if (t.getElectricDeviceValue() != null) {
                                for (ElectricCurrentMeteringValueExportType e :
                                        t.getElectricDeviceValue().getValues().getCurrentValue()) {
                                    // проверить сохранены ли уже показания
                                    if (Utl.between(Utl.getDateFromXmlGregCal(e.getEnterIntoSystem()), dt1, dt2) &&
                                            !meterMng.getIsMeterDataExist(mdLst, t.getMeteringDeviceRootGUID(),
                                                    e.getEnterIntoSystem()) && e.getMeteringValueT1() != null) {
                                        log.trace("показания по ElectricDeviceValue: GUID={} date={}, enter={}, val={}",
                                                t.getMeteringDeviceRootGUID(), e.getDateValue(), e.getEnterIntoSystem(),
                                                e.getMeteringValueT1());
                                        // сохранить показание по счетчику в базу
                                        saveMeterData(meterEol, e.getMeteringValueT1(), e.getEnterIntoSystem(), period);
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
     * @param meter  - счетчик
     * @param num1   - показание
     * @param ts     - timestamp
     * @param period - период для T_OBJXPAR
     */
    private void saveMeterData(Eolink meter, String num1, XMLGregorianCalendar ts, String period) throws UnusableCode {
        // усечь до секунд
        Date dt = Utl.truncDateToSeconds(Utl.getDateFromXmlGregCal(ts));
        // погасить ошибку записи в базу
        soapConfig.saveError(meter, CommonErrs.ERROR_WHILE_SAVING_DATA, false);
        StoredProcedureQuery qr;
        qr = em.createStoredProcedureQuery("scott.p_meter.ins_data_meter");
        qr.registerStoredProcedureParameter(1, Integer.class,
                ParameterMode.IN); // p_met_klsk
        qr.registerStoredProcedureParameter(2, Double.class,
                ParameterMode.IN); // p_n1
        qr.registerStoredProcedureParameter(3, Date.class,
                ParameterMode.IN); // p_ts
        qr.registerStoredProcedureParameter(4, String.class,
                ParameterMode.IN);
        qr.registerStoredProcedureParameter(5, Integer.class,
                ParameterMode.OUT);
        qr.setParameter(1, meter.getKoObj().getId());
        qr.setParameter(2, Double.valueOf(num1));
        qr.setParameter(3, dt);
        qr.setParameter(4, period);
        qr.execute();
        Integer ret = (Integer) qr.getOutputParameterValue(5);
        if (!ret.equals(0)) {
            soapConfig.saveError(meter, CommonErrs.ERROR_WHILE_SAVING_DATA, true);
        }
        log.trace("Результат исполнения scott.p_meter.ins_data_meter={}", ret);
    }

    /**
     * Записать показание по счетчику
     *
     * @param meter      - счетчик
     * @param val        - принятое от ГИС показание
     * @param dtVal      - дата снятия
     * @param dtEnter    - дата внесения в ГИС
     * @param orgGUID    - организация которая внесла
     * @param readingSrc - кем внесено
     */
    private void saveVal(Eolink meter, String val, Date dtVal,
                         Date dtEnter, String orgGUID, String readingSrc,
                         NsiRef munRes)
            throws WrongGetMethod, IOException, WrongParam {
        if (val != null) {
            Double valD = Double.parseDouble(val);
            Ulist lst = null;
            if (munRes != null) {
                lst = ulistDao.getListElemByGUID(munRes.getGUID());
            }
            MeterVal meterVal = MeterVal.MeterValBuilder.aMeterVal()
                    .withDtEnter(dtEnter)
                    .withEolink(meter)
                    .withOrgGuid(orgGUID)
                    .withReadingSource(readingSrc)
                    .withUlist(lst)
                    .withVal(new BigDecimal(val))
                    .build();
            em.persist(meterVal);
        }
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

    /**
     * Проверить наличие заданий на выгрузку показаний по счетчикам, по домам
     * и если их нет, - создать
     */

    public void checkPeriodicTask(Integer taskId) {
        Task task = em.find(Task.class, taskId);
        // создать по всем домам задания, если их нет
        createTask("GIS_EXP_METER_VALS", "SYSTEM_RPT_MET_EXP_VAL", "STP", "Дом",
                "выгрузку показаний приборов учета");
        // Установить статус выполнения задания
        task.setState("ACP");
    }

    private void createTask(String actTp, String parentCD, String state, String eolTp, String purpose) {
        int a;// создавать по 100 штук, иначе -блокировка Task (нужен коммит)
        a = 1;
        for (Eolink e : eolinkDao.getEolinkByTpWoTaskTp(eolTp, actTp, parentCD)) {
            // статус - STP, остановлено (будет запускаться другим заданием)
            Task newTask = ptb.setUp(e, null, actTp, state, 1);
            // добавить как зависимое задание к системному повторяемому заданию
            ptb.addAsChild(newTask, parentCD);
            ptb.save(newTask);
            log.info("Добавлено задание на {}, по объекту {}, Eolink.id={}", purpose, eolTp, e.getId());
            a++;
            if (a++ >= 100) {
                break;
            }
        }
    }


}
