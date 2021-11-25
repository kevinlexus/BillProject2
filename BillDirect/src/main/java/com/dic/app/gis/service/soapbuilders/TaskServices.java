package com.dic.app.gis.service.soapbuilders;

import com.dic.bill.model.exs.Task;
import com.dic.bill.model.exs.TaskPar;
import com.ric.cmn.excp.WrongGetMethod;

public interface TaskServices {

    void activateRptTask(Task task) throws WrongGetMethod;

    TaskPar getTrgTask(Task task);

    void setProcTask(TaskPar taskPar);
}
