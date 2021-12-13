package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.maintaners.UlistMng;
import com.dic.app.gis.service.soapbuilders.TaskServices;
import com.dic.app.gis.service.soapbuilders.impl.*;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.TaskPar;
import com.ric.cmn.Utl;
import com.ric.cmn.excp.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import javax.xml.datatype.DatatypeConfigurationException;
import java.io.IOException;
import java.text.ParseException;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskProcessor {


    private final TaskMng taskMng;
    private final EntityManager em;
    private final HouseManagementAsyncBindingBuilder hb;
    private final HcsOrgRegistryAsyncBindingBuilder os;
    private final HcsOrgRegistryAsyncBindingSimpleBuilder osSimple;
    private final DeviceMeteringAsyncBindingBuilder dm;
    private final HcsBillsAsyncBuilder bill;
    private final TaskServices tb;
    private final UlistMng ulistMng;
    private final NsiServiceAsyncBindingBuilder nsiSv;

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void processTask(Integer taskId) {
        // получить задание заново (могло измениться в базе) - WTF??? ред.05.09.2019
        Task task = em.find(Task.class, taskId);
        log.info("Обработка задания ID={}, CD={}, ActCD={}",
                task.getId(), task.getCd(), task.getAct().getCd());
        if (Utl.in(task.getState(), "INS")) {
            taskMng.clearLagAndNextStart(task);
        }
        if (Utl.in(task.getState(), "INS", "RPT") || Utl.in(task.getState(), "ACK") && task.isActivate()) {
            // Почистить результаты задания
            taskMng.clearAllResult(task);
            String actCd = task.getAct().getCd();
            String state = task.getState();

            try {
                process(task, actCd, state);
            } catch (ErrorProcessAnswer | DatatypeConfigurationException | CantPrepSoap e) {
                e.printStackTrace();
                log.error("Ошибка при отправке задания Task.id={}, message={}", task.getId(),
                        e.getMessage());
                taskMng.setState(task, "ERR");
                taskMng.setResult(task, e.getMessage());
            } catch (Exception e) {
                log.error("Ошибка выполнения задания Task.id={}, message={}", task.getId(),
                        Utl.getStackTraceString(e));
                String errMess = StringUtils.substring(Utl.getStackTraceString(e), 0, 1000);
                if (!task.getAct().getCd().equals("GIS_SYSTEM_RPT")) {
                    // не помечать ошибкой системные, повторяемые задания
                    taskMng.setState(task, "ERR");
                }
                taskMng.setResult(task, errMess);
            }
        } else {
            log.warn("Задание не было активировано");
        }

    }

    private void process(Task task, String actCd, String state) throws WrongParam, WrongGetMethod, IOException, CantSendSoap, CantPrepSoap, UnusableCode, ErrorProcessAnswer, DatatypeConfigurationException, ParseException, CantUpdNSI {
        // Выполнить задание
        switch (actCd) {
            case "GIS_SYSTEM_CHECK":
                // Системные задания проверок
                if (state.equals("INS")) {
                    switch (task.getCd()) {
                        case "SYSTEM_CHECK_HOUSE_EXP_TASK":
                            // Проверка наличия заданий по экспорту объектов дома
                            hb.checkPeriodicHouseExp(task.getId());
                            break;
                        case "SYSTEM_CHECK_MET_VAL_TASK":
                            // Проверка наличия заданий по экспорту показаний счетчиков по помещениям дома
                            dm.checkPeriodicTask(task.getId());
                            break;
                        case "SYSTEM_CHECK_ORG_EXP_TASK":
                            // Проверка наличия заданий по экспорту параметров организаций
                            os.checkPeriodicTask(task.getId());
                            break;
                        case "SYSTEM_CHECK_REF_EXP_TASK":
                            // Проверка наличия заданий по экспорту справочников организации
                            nsiSv.checkPeriodicTask(task.getId());
                            break;
                        case "SYSTEM_CHECK_IMP_PD":
                            // Проверка наличия заданий по импорту ПД
                            bill.checkPeriodicImpExpPd(task.getId());
                            break;
                    }
                }
                break;
            case "GIS_SAVE_FILE_VALS":
                // Выгрузка показаний приборов учета в файл
                if (state.equals("INS")) {
                    dm.saveValToFile(task.getId());
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
                if (state.equals("INS")) {
                    // Обновление объектов дома
                    hb.importHouseUOData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.importHouseUODataAsk(task.getId());
                }

                break;
            case "GIS_EXP_HOUSE":
                // Экспорт из ГИС ЖКХ объектов дома
                if (state.equals("INS")) {
                    // Экспорт объектов дома
                    hb.exportHouseData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.exportHouseDataAsk(task.getId());
                }
                break;
            case "GIS_EXP_ACCS":
                // Экспорт из ГИС ЖКХ лиц.счетов
                if (state.equals("INS")) {
                    hb.exportAccountData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.exportAccountDataAsk(task.getId());
                }
                break;
            case "GIS_EXP_METERS":
                // Экспорт из ГИС ЖКХ приборов учета
                if (state.equals("INS")) {
                    hb.exportDeviceData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.exportDeviceDataAsk(task.getId());
                }
                break;
            case "GIS_IMP_ACCS":
                if (state.equals("INS")) {
                    // Импорт лицевых счетов
                    hb.importAccountData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.importAccountDataAsk(task.getId());
                }
                break;
            case "GIS_IMP_METERS":
                // todo нет реализации пока
                break;
            case "GIS_IMP_METER_VALS":
                if (state.equals("INS")) {
                    // Импорт показаний счетчиков
                    dm.importMeteringDeviceValues(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    dm.importMeteringDeviceValuesAsk(task.getId());
                }
                break;
            case "GIS_EXP_METER_VALS":
                if (state.equals("INS")) {
                    // экспорт показаний счетчиков
                    dm.exportMeteringDeviceValues(task.getId());
                } else if (state.equals("ACK")) {
                    // запрос ответа
                    dm.exportMeteringDeviceValuesAsk(task.getId());
                }
                break;
            case "GIS_IMP_PAY_DOCS":
                if (state.equals("INS")) {
                    // Импорт платежных документов по дому
                    bill.importPaymentDocumentData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    bill.importPaymentDocumentDataAsk(task.getId());
                }
                break;
            case "GIS_EXP_PAY_DOCS":
                if (state.equals("INS")) {
                    // экспорт платежных документов по дому
                    bill.exportPaymentDocumentData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    bill.exportPaymentDocumentDataAsk(task.getId());
                }
                break;
            case "GIS_EXP_ORG":
                // Экспорт параметров организации
                if (state.equals("INS")) {
                    osSimple.exportOrgRegistry(task.getId());
                } else if (state.equals("ACK")) {
                    osSimple.exportOrgRegistryAsk(task.getId());
                }
                break;
            case "GIS_EXP_DATA_PROVIDER_NSI_ITEM":
                if (state.equals("INS")) {
                    // Экспорт внутреннего справочника организации
                    nsiSv.exportDataProviderNsiItem(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    nsiSv.exportDataProviderNsiItemAsk(task.getId());
                }
                break;
            case "GIS_EXP_COMMON_NSI_ITEM":
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
                    //bill.exportNotificationsOfOrderExecution(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    //bill.exportNotificationsOfOrderExecutionAsk(task.getId());
                }
                break;
            default:
                taskMng.setResult(task, "Ошибка! Нет обработчика по заданию");
                taskMng.setState(task, "ERR");
                break;
        }
    }
}
