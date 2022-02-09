package com.dic.app.gis.service.maintaners;

import com.dic.bill.model.exs.Eolink;
import com.dic.bill.model.exs.Task;
import org.springframework.transaction.annotation.Transactional;

import java.util.List;

public interface TaskMng {

	void setState(Task task, String state);
	void setResult(Task task, String result);
	void clearAllResult(Task task);
	void setEolinkIdf(Eolink eo, String guid, String un, Integer status);
	Task getByTguid(Task task, String tguid);
	void logTask(Task task, boolean isStart, Boolean isSucc);
    void alterDtNextStart(Task task);

    void clearLagAndNextStart(Task task);

    @Transactional
    void putTaskToWorkByDebtRequestId(List<Integer> debRequestId);
}