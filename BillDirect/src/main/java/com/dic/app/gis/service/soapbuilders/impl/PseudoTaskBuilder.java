package com.dic.app.gis.service.soapbuilders.impl;

import com.dic.app.gis.service.maintaners.TaskEolinkParMng;
import com.dic.bill.dao.ParDAO;
import com.dic.bill.dao.TaskDAO;
import com.dic.bill.mm.LstMng;
import com.dic.bill.model.bs.Lst2;
import com.dic.bill.model.bs.Par;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.TaskPar;
import com.dic.bill.model.exs.TaskToTask;
import com.ric.cmn.excp.WrongParam;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import javax.persistence.EntityManager;
import java.util.Date;


/**
 * Построитель заданий
 *
 * @author lev
 * @version 1.00
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class PseudoTaskBuilder {
    private final LstMng lstMng;
    private final ParDAO parDao;
    private final TaskEolinkParMng teParMng;
    private final EntityManager em;
    private final TaskDAO taskDao;

    /* инициализация
     * @param eolink - объект к которому привязано задание
     * @param parent - родительское задание (необязательный параметр)
     * @param actCd - тип задания
     * @param state - статус состояния
     */

    public Task setUp(Eolink eolink, Task parent, String actCd, String state, Integer userId) {
        return setUp(eolink, parent, null, actCd, state, userId, null);
    }

    /* инициализация
     * @param eolink - объект к которому привязано задание
     * @param parent - родительское задание по PARENT_ID (необязательный параметр)
     * @param master - ведущее задание по DEP_ID (необязательный параметр)
     * @param actCd - тип задания
     * @param state - статус состояния
     */

    public Task setUp(Eolink eolink, Task parent, Task master, String actCd, String state,
                      Integer userId, Eolink procUk) {
        Lst2 actVal = lstMng.getByCD(actCd);
        return Task.builder()
                .withEolink(eolink)
                .withParent(parent)
                .withMaster(master)
                .withState(state)
                .withAct(actVal)
                .withFk_user(userId)
                //.withErrAckCnt(0)
                .withProcUk(procUk)
                .withTrace(0).build();
    }

    /**
     * добавить параметр
     *
     * @param parCd - CD параметра
     * @param n1    - значение Double
     * @param s1    - значение String
     * @param b1    - значение Boolean
     * @param d1    - значение Date
     */

    public void addTaskPar(Task task, String parCd, Double n1, String s1, Boolean b1, Date d1) throws WrongParam {
        Par par = parDao.getByCd(-1, parCd);
        if (!par.getDataTp().equals("SI")) {
            throw new WrongParam("Некорректное использоваение параметра =" + parCd + " тип - не SI");
        }

        Double valN1 = null;
        String valS1 = null;
        Date valD1 = null;

        if (par.getTp().equals("NM")) {
            if (s1 != null || b1 != null || d1 != null) {
                throw new WrongParam("Параметр =" + parCd + " имеет тип NM!");
            }
            valN1 = n1;
        } else if (par.getTp().equals("ST")) {
            if (n1 != null || b1 != null || d1 != null) {
                throw new WrongParam("Параметр =" + parCd + " имеет тип ST!");
            }
            valS1 = s1;
        } else if (par.getTp().equals("BL")) {
            if (n1 != null || s1 != null || d1 != null) {
                throw new WrongParam("Параметр =" + parCd + " имеет тип BL!");
            }
            if (b1) {
                valN1 = 1D;
            } else {
                valN1 = 0D;
            }

        } else if (par.getTp().equals("DT")) {
            if (n1 != null || s1 != null || b1 != null) {
                throw new WrongParam("Параметр =" + parCd + " имеет тип Dt!");
            }
            valD1 = d1;
        }

        TaskPar taskPar = new TaskPar(task, par, valN1, valS1, valD1);
        task.getTaskPar().add(taskPar);
    }

    // переписать параметры в объект Eolink

    public void saveToEolink(Task task) {
        em.persist(task);
        teParMng.acceptPar(task);
    }


    public void save(Task task) {
        em.persist(task);
    }

    /**
     * Добавить задание как зависимое, в список выполнения другого задания
     *
     * @param cd - CD ведущее задания
     */

    @Transactional(propagation = Propagation.REQUIRES_NEW, rollbackFor = Exception.class)
    public void addAsChild(Task task, String cd) {
        Task parent = taskDao.getByCd(cd);
        //log.info("**** 6");
        log.info("******* Прикреплено дочернее задание к родительскому Parent Task.id={}", parent.getId());
        Lst2 lst = lstMng.getByCD("Связь повторяемого задания");
        //log.info("**** 7");
        TaskToTask t = new TaskToTask(parent, task, lst);
        //log.info("**** 8");
        task.getOutside().add(t);
        //log.info("**** 9");
        //log.info("******* parent.id={}, getInside().size()={}", parent.getId(), parent.getInside().size());
    }

    /**
     * Добавить задание как зависимое, в список выполнения другого задания
     *
     * @param parent - ведущее задание
     */

    public void addAsChild(Task task, Task parent) {
        Lst2 lst = lstMng.getByCD("Связь повторяемого задания");
        TaskToTask t = new TaskToTask(parent, task, lst);
        task.getOutside().add(t);
    }

}

