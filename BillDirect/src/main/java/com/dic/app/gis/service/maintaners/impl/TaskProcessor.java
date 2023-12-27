package com.dic.app.gis.service.maintaners.impl;

import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.app.gis.service.soapbuilders.impl.*;
import com.dic.app.utils.LockByKey;
import com.dic.app.utils.LockContainer;
import com.dic.bill.model.exs.Task;
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
import java.net.SocketException;
import java.text.ParseException;

@Slf4j
@Service
@RequiredArgsConstructor
public class TaskProcessor {


    private final TaskMng taskMng;
    private final UlistMng ulistMng;
    private final EntityManager em;
    private final HouseManagementAsyncBindingBuilder hb;
    private final DebtRequestsServiceAsyncBindingBuilder db;
    private final HcsOrgRegistryAsyncBindingSimpleBuilder osSimple;
    private final DeviceMeteringAsyncBindingBuilder dm;
    private final NsiServiceAsyncBindingBuilder nsiSv;
    private final HcsBillsAsyncBuilder bill;

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void processTask(Integer taskId) {
        // получить задание заново (могло измениться в базе)
        Task task = em.find(Task.class, taskId);
        log.trace("Обработка задания ID={}, CD={}, ActCD={}",
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
            } catch (SocketException e) {
                // не помечать ошибкой, если SocketException, пусть отправка повторится
                log.error("Ошибка при отправке задания Task.id={}", task.getId(), e);
            } catch (ErrorProcessAnswer | DatatypeConfigurationException | CantPrepSoap e) {
                log.error("Ошибка при отправке задания Task.id={}", task.getId(), e);
                taskMng.setState(task, "ERR");
                taskMng.setResult(task, e.getMessage());
            } catch (Exception e) {
                log.error("Ошибка при отправке задания Task.id={}", task.getId(), e);
                String errMess = StringUtils.substring(Utl.getStackTraceString(e), 0, 1000);
                if (!task.getAct().getCd().equals("GIS_SYSTEM_RPT")) {
                    // не помечать ошибкой системные, повторяемые задания
                    taskMng.setState(task, "ERR");
                }
                taskMng.setResult(task, errMess);
            }
        } else {
            log.error("******** ЗАДАНИЕ ID={}, CD={}, ActCD={}, State={}, НЕ БЫЛО ОТПРАВЛЕНО НА ВЫПОЛНЕНИЕ ********",
                    task.getId(), task.getCd(), task.getAct().getCd(), task.getState());
        }
    }

    private void process(Task task, String actCd, String state) throws WrongParam, WrongGetMethod, IOException, CantSendSoap, CantPrepSoap, UnusableCode, ErrorProcessAnswer, DatatypeConfigurationException, ParseException, CantUpdNSI {
        LockByKey lockByKey = LockContainer.lockHouseByKey;
        try {
            lockByKey.lock(String.valueOf(task.getEolink().getId()));
            // Выполнить задание
            switch (actCd) {
                case "GIS_SAVE_FILE_VALS":
                    // Выгрузка показаний приборов учета в файл
                    if (state.equals("INS")) {
                        dm.saveValToFile(task.getId());
                    }
                    break;
/*
            case "GIS_UPD_HOUSE": // todo не вызывается и не создается нигде задание
                // Импорт объектов дома
                if (state.equals("INS")) {
                    // Обновление объектов дома
                    hb.importHouseUOData(task.getId());
                } else if (state.equals("ACK")) {
                    // Запрос ответа
                    hb.importHouseUODataAsk(task.getId());
                }
                break;
*/
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
                case "GIS_EXP_DEB_SUB_REQUEST":
                    // Экспорт из ГИС ЖКХ запросы по задолженностям
                    if (state.equals("INS")) {
                        db.exportDebtSubrequest(task.getId());
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        db.exportDebtSubrequestAsk(task.getId());
                    }
                    break;
                case "GIS_IMP_DEB_SUB_RESPONSE":
                    // Экспорт из ГИС ЖКХ запросы по задолженностям
                    if (state.equals("INS")) {
                        db.importDebtSubrequestResponse(task.getId());
                    } else if (state.equals("ACK")) {
                        // Запрос ответа
                        db.importDebtSubrequestResponseAsk(task.getId());
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
                        ulistMng.loadNsi(task.getId(), "NSI");
                        ulistMng.loadNsi(task.getId(), "NSIRAO");
                        taskMng.setState(task, "ACP");
                    }
                    break;
                default:
                    taskMng.setResult(task, "Ошибка! Нет обработчика по заданию");
                    taskMng.setState(task, "ERR");
                    break;
            }
        } catch (Exception e) {
            lockByKey.unlock(String.valueOf(task.getEolink().getId()));
            throw e;
        } finally {
            lockByKey.unlock(String.valueOf(task.getEolink().getId()));
        }
    }
}
