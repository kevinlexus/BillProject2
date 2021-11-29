package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.UlistMng;
import com.dic.app.gis.service.soapbuilders.*;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.TaskPar;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.text.ParseException;

@Slf4j
@Service
public class TaskControllerProcessor {

    @Autowired
    private TaskMng taskMng;
    @PersistenceContext
    private EntityManager em;
    @Autowired
    private HouseManagementAsyncBindingBuilders hb;
    @Autowired
    private HcsOrgRegistryAsyncBindingBuilders os;
    @Autowired
    private HcsOrgRegistryAsyncBindingSimpleBuilders osSimple;
    @Autowired
    private DeviceMeteringAsyncBindingBuilders dm;
    @Autowired
    private HcsBillsAsyncBuilders bill;
    @Autowired
    private HcsPaymentAsyncBuilders pay;
    @Autowired
    private TaskServices tb;
    @Autowired
    private UlistMng ulistMng;
    @Autowired
    private NsiServiceAsyncBindingBuilders nsiSv;

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void processTask(Task someTask) throws WrongParam, WrongGetMethod, IOException, CantPrepSoap, CantSendSoap, UnusableCode, ErrorProcessAnswer, DatatypeConfigurationException, ParseException, CantUpdNSI, ErrorWhileDist {
        // получить задание заново (могло измениться в базе) - WTF??? ред.05.09.2019
        Task task = em.find(Task.class, someTask.getId());
        log.trace("Обработка задания ID={}, CD={}, ActCD={}",
                task.getId(), task.getCd(), task.getAct().getCd());
        if (Utl.in(task.getState(), "INS", "ACK", "RPT") && task.isActivate()) {
            // Почистить результаты задания
            taskMng.clearAllResult(task);
            String actCd = task.getAct().getCd();
            String state = task.getState();

            // Выполнить задание
            switch (actCd) {
                case "GIS_SYSTEM_CHECK":
                    // Системные задания проверок
                    if (state.equals("INS")) {
                        switch (task.getCd()) {
                            case "SYSTEM_CHECK_HOUSE_EXP_TASK":
                                // Проверка наличия заданий по экспорту объектов дома
                                hb.checkPeriodicHouseExp(task);
                                break;
                            case "SYSTEM_CHECK_MET_VAL_TASK":
                                // Проверка наличия заданий по экспорту показаний счетчиков по помещениям дома
                                dm.checkPeriodicTask(task);
                                break;
                            case "SYSTEM_CHECK_ORG_EXP_TASK":
                                // Проверка наличия заданий по экспорту параметров организаций
                                os.checkPeriodicTask(task);
                                break;
                            case "SYSTEM_CHECK_REF_EXP_TASK":
                                // Проверка наличия заданий по экспорту справочников организации
                                nsiSv.checkPeriodicTask(task);
                                break;
                            case "SYSTEM_CHECK_IMP_PD":
                                // Проверка наличия заданий по импорту ПД
                                bill.checkPeriodicImpExpPd(task);
                                break;
                            case "SYSTEM_CHECK_IMP_SUP_NOTIF":
                                // Проверка наличия заданий по импорту Извещений по ПД
                                pay.checkPeriodicSupplierImpNotif(task);
                                break;
                            case "SYSTEM_CHECK_IMP_SUP_NOTIF_CANCEL":
                                // Проверка наличия заданий по импорту отмены Извещений по ПД
                                pay.checkPeriodicImpCancelNotif(task);
                                break;
                        }
                    }
                    break;
                case "GIS_SAVE_FILE_VALS":
                    // Выгрузка показаний приборов учета в файл
                    if (state.equals("INS")) {
                        dm.saveValToFile(task);
                    }
                    break;
                case "GIS_SYSTEM_RPT":
                    // Запуск повторяемого задания, если задано
                    TaskPar taskPar = tb.getTrgTask(task);
                    if (taskPar != null) {
                        // активировать все зависимые задания
                        log.trace("******* Активировано повторяемое задание Task.id={}", task.getId());
                        tb.activateRptTask(task);
                        // добавить в список выполненных заданий
                        tb.setProcTask(taskPar);
                        // пометить статус повторяемого выполнения, на случай, если запускалось в ручную state--> "INS"
                        if (task.getState().equals("INS")) {
                            taskMng.setState(task, "RPT");
                        }
                    }
                    break;
                case "GIS_UPD_HOUSE":
                    // Импорт объектов дома
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        // Обновление объектов дома
                        hb.importHouseUOData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.importHouseUODataAsk(task);
                    }

                    break;
                case "GIS_EXP_CACH_DATA":
                    // Экспорт из ГИС ЖКХ уставов УК
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        // Экспорт уставов
                        hb.exportCaChData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.exportCaChDataAsk(task);
                    }
                    break;
                case "GIS_EXP_HOUSE":
                    // Экспорт из ГИС ЖКХ объектов дома
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        // Экспорт объектов дома
                        hb.exportHouseData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.exportHouseDataAsk(task);
                    }
                    break;
                case "GIS_EXP_ACCS":
                    // Экспорт из ГИС ЖКХ лиц.счетов
                    if (state.equals("INS")) {
                        hb.exportAccountData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.exportAccountDataAsk(task);
                    }
                    break;
                case "GIS_EXP_BRIEF_SUPPLY_RES_CONTRACT":
                    // Экспорт из ГИС ЖКХ сокращенного состава информации о договоре ресурсоснабжения
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        hb.exportBriefSupplyResourceContract(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.exportBriefSupplyResourceContractAsk(task);
                    }
                    break;
                case "GIS_EXP_METERS":
                    // Экспорт из ГИС ЖКХ приборов учета
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        hb.exportDeviceData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.exportDeviceDataAsk(task);
                    }
                    break;
                case "GIS_IMP_ACCS":
                    hb.setUp(task);
                    if (state.equals("INS")) {
                        // Импорт лицевых счетов
                        hb.importAccountData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        hb.importAccountDataAsk(task);
                    }
                    break;
                case "GIS_IMP_METERS":
                    // todo нет реализации пока
                    break;
                case "GIS_IMP_METER_VALS":
                    dm.setUp(task);
                    if (state.equals("INS")) {
                        // Импорт показаний счетчиков
                        dm.importMeteringDeviceValues(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        dm.importMeteringDeviceValuesAsk(task);
                    }
                    break;
                case "GIS_EXP_METER_VALS":
                    dm.setUp(task);
                    if (state.equals("INS")) {
                        // экспорт показаний счетчиков
                        dm.exportMeteringDeviceValues(task);
                    } else if (state.equals("ACK")) {
                        // запрос ответа
                        dm.exportMeteringDeviceValuesAsk(task);
                    }
                    break;
                case "GIS_IMP_PAY_DOCS":
                    //bill.setUp(task);
                    if (state.equals("INS")) {
                        // Импорт платежных документов по дому
                        bill.importPaymentDocumentData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        bill.importPaymentDocumentDataAsk(task);
                    }
                    break;
                case "GIS_EXP_PAY_DOCS":
                    //bill.setUp(task);
                    if (state.equals("INS")) {
                        // экспорт платежных документов по дому
                        bill.exportPaymentDocumentData(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        bill.exportPaymentDocumentDataAsk(task);
                    }
                    break;
                case "GIS_IMP_SUP_NOTIFS":
                    pay.setUp(task);
                    if (state.equals("INS")) {
                        // Импорт извещений исполнения распоряжений
                        pay.importSupplierNotificationsOfOrderExecution(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        pay.importSupplierNotificationsOfOrderExecutionAsk(task);
                    }
                    break;

                case "GIS_IMP_CANCEL_NOTIFS":
                    // Экспорт отмены извещений исполнения документа
                    pay.setUp(task);
                    if (state.equals("INS")) {
                        // Экспорт отмены извещений исполнения документа
                        pay.importNotificationsOfOrderExecutionCancelation(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        pay.importNotificationsOfOrderExecutionCancelationAsk(task);
                    }
                    break;
                case "GIS_EXP_PAY_DETAIL_DOCS":
                    pay.setUp(task);
                    if (state.equals("INS")) {
                        // экспорт детализации платежного документа
                        pay.exportPaymentDocumentDetails(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        pay.exportPaymentDocumentDetailsAsk(task);
                    }
                    break;
                case "GIS_EXP_ORG":
                    // Экспорт данных организации
                    osSimple.setUp(task);
                    if (state.equals("INS")) {
                        osSimple.exportOrgRegistry(task);
                    } else if (state.equals("ACK")) {
                        osSimple.exportOrgRegistryAsk(task);
                    }
                    break;
                case "GIS_EXP_DATA_PROVIDER":
                    // Экспорт сведений о поставщиках данных
                    os.setUp(task);
                    if (state.equals("INS")) {
                        os.exportDataProvider(task);
                    } else if (state.equals("ACK")) {
                        os.exportDataProviderAsk(task);
                    }
                    break;
                case "GIS_EXP_DATA_PROVIDER_NSI_ITEM":
                    nsiSv.setUp(task, false);
                    if (state.equals("INS")) {
                        // Экспорт внутреннего справочника организации
                        nsiSv.exportDataProviderNsiItem(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        nsiSv.exportDataProviderNsiItemAsk(task);
                    }
                    break;
                case "GIS_EXP_COMMON_NSI_ITEM":
                    nsiSv.setUp(task, true);
                    if (state.equals("INS")) {
                        // Экспорт общих справочников
                        // note Внимание! в task.eolink заполнять любую УК, так как ppguid будет по РКЦ!
                        ulistMng.loadNsi("NSI");
                        ulistMng.loadNsi("NSIRAO");
                        taskMng.setState(task, "ACP");
                    }
                    break;
                case "GIS_EXP_NOTIF_1":
                case "GIS_EXP_NOTIF_8":
                case "GIS_EXP_NOTIF_16":
                case "GIS_EXP_NOTIF_24":
                    // Экспорт извещений исполнения документа по дням выгрузки
                    //bill.setUp(task);
                    if (state.equals("INS")) {
                        // Экспорт извещений исполнения документа
                        bill.exportNotificationsOfOrderExecution(task);
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        bill.exportNotificationsOfOrderExecutionAsk(task);
                    }
                    break;
                default:
                    log.error("Ошибка! Нет обработчика по заданию с типом={}", actCd);
                    break;
            }

        }

    }
}
