package com.dic.app.gis.service.maintaners.impl;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;
import javax.persistence.Transient;

import com.dic.bill.dao.TaskDAO2;
import com.ric.cmn.Utl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Propagation;
import org.springframework.transaction.annotation.Transactional;

import com.dic.bill.dao.TaskDAO;
import com.dic.app.gis.service.maintaners.TaskMng;
import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;

import lombok.extern.slf4j.Slf4j;

import java.util.Calendar;
import java.util.GregorianCalendar;
import java.util.List;


@Service
@Slf4j
@Transactional
public class TaskMngImpl implements TaskMng {

    @PersistenceContext
    private EntityManager em;
    @Autowired
    private TaskDAO taskDao;
    @Autowired
    private TaskDAO2 taskDao2;

    /**
     * Установить статус задания
     */
	@Override
    public void setState(Task task, String state) {
    	Task foundTask = em.find(Task.class, task.getId());
		foundTask.setState(state);
	}

    /**
     * Установить результат задания
     */
	@Override
    public void setResult(Task task, String result) {
    	Task foundTask = em.find(Task.class, task.getId());
		foundTask.setResult(result);
	}

    /**
     * Очистить результат в т.ч. дочерних заданий
     */
	@Override
    public void clearAllResult(Task task) {
    	Task foundTask = em.find(Task.class, task.getId());
    	setResult(foundTask, null);
    	foundTask.getChild().stream().forEach(t-> {
    		setResult(t, null);
    	});
	}

	/**
	 * Установить идентификаторы объектов (если не заполненны)
	 * @param eo - Объект
	 * @param guid - GUID, полученный от ГИС
	 * @param un - уникальный номер, полученный от ГИС
	 * @param status - статус
	 */
	@Override
	public void setEolinkIdf(Eolink eo, String guid, String un, Integer status) {
		if (eo.getGuid() == null) {
			eo.setGuid(guid);
		}
		if (eo.getUn() == null) {
			eo.setUn(un);
		}
		if (!eo.getStatus().equals(status) ) {
			eo.setStatus(status);
		}

	}

	/**
	 * Вернуть задание по ID родительского задания и транспортному GUID
	 * @param - task - родительское задание
	 * @param - tguid - транспортный GUID
	 */
	@Override
	public Task getByTguid(Task task, String tguid) {

		return taskDao.getByTguid(task, tguid);

	}

	/**
	 * Добавить в лог сообщение
	 * @param task - задание
	 * @param isStart - начало - окончание процесса
	 * @param isSucc - успешно / с ошибкой
	 */
	@Override
	public void logTask(Task task, boolean isStart, Boolean isSucc) {
		if (isSucc!=null) {
			log.info("Task.id={}, {}, {}, {}, {}",
					task.getId(), task.getAct().getName(), task.getState(),
					isStart?"Начало":"Окончание", isSucc?"Выполнено":"ОШИБКА");
		} else {
			log.info("Task.id={}, {}, {}, {}",
					task.getId(), task.getAct().getName(), task.getState(),
					isStart?"Начало":"Окончание");
		}
	}


	/**
	 * Установить время следующего старта задания
	 */
	@Override
	public void alterDtNextStart(Task task) {
		GregorianCalendar cal = new GregorianCalendar();
		int lag2 = Utl.nvl(task.getLagNextStart(), 0) + 600; // 10 минут
		// не более 60 минут
		if (lag2 > 3600) {
			lag2 = 3600;
		}
		cal.add(Calendar.SECOND, lag2);

		log.trace("задержка вызова задания taskId={}, увеличена до {}", task.getId(), lag2);
		task.setLagNextStart(lag2);
		task.setDtNextStart(cal.getTime());
	}

	@Override
	public void clearLagAndNextStart(Task task) {
		task.setLagNextStart(null);
		task.setDtNextStart(null);
	}


	@Override
	@Transactional
	public void putTaskToWorkByDebtRequestId(List<Integer> debRequestId) {
		List<Task> tasks = taskDao2.findDistinctActiveTaskIdByDebRequestIds(debRequestId);
		tasks.forEach(t -> t.setState("INS"));
	}

}