package com.migu.schedule;

import com.migu.schedule.constants.ReturnCodeKeys;
import com.migu.schedule.info.TaskInfo;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/*
*类名和方法不能修改
 */
public class Schedule
{
    
    /**
     * 负载均衡阈值
     */
    private int thresholdValue = 10;
    
    /**
     * 任务状态：运行中
     */
    private int TASK_STATUS_RUNNING = 1;
    
    /**
     * 任务状态：挂起
     */
    private int TASK_STATUS_hangup = 0;
    
    
    private List<Integer> nodeList = new ArrayList<Integer>();
    
    
    private List<Integer> taskList = new ArrayList<Integer>();
    
    private Map<Integer, List<TaskInfo>> taskStatusMap = new ConcurrentHashMap<Integer, List<TaskInfo>>();
    
    private Map<Integer, Integer> taskMap = new ConcurrentHashMap<Integer, Integer>();
    
    private Map<Integer, List<Integer>> sameTasksMap = new ConcurrentHashMap<Integer, List<Integer>>();
    
    
    
    
    
    Comparator<TaskInfo> comparator = new Comparator<TaskInfo>()
    {
        public int compare(TaskInfo o1, TaskInfo o2)
        {
            return (o1.getTaskId() - o2.getTaskId());
        }
    };
    
    Comparator<TaskInfo> comparatorByNodeId = new Comparator<TaskInfo>()
    {
        public int compare(TaskInfo o1, TaskInfo o2)
        {
            return (o1.getNodeId() - o2.getNodeId());
        }
    };
    
    Comparator<Integer> comparatorByTime = new Comparator<Integer>()
    {
        public int compare(Integer o1, Integer o2)
        {
            return (taskMap.get(o2) - taskMap.get(o1));
        }
    };
    
    
    /**
     * 
     * 系统初始化，会清空所有数据，包括已经注册到系统的服务节点信息、以及添加的任务信息，
     * 全部都被清理。执行该命令后，系统恢复到最初始的状态。
     *
     * @author zl_wanglei
     * @return
     */
    public int init()
    {
        nodeList.clear();
        taskList.clear();
        taskStatusMap.clear();
        taskMap.clear();
        sameTasksMap.clear();
        
        return ReturnCodeKeys.E001;
    }
    
    /**
     * 服务节点注册进本系统
     * @author zl_wanglei
     * @param nodeId
     * @return
     */
    public int registerNode(int nodeId)
    {
        
        if (nodeId < 0)
        {
            return ReturnCodeKeys.E004;
        }
        
        if (nodeList.contains(nodeId))
        {
            return ReturnCodeKeys.E005;
        }
        
        nodeList.add(nodeId);
        Collections.sort(nodeList);
        
        return ReturnCodeKeys.E003;
    }
    
    /**
     * 服务节点注销
     * @author zl_wanglei
     * @param nodeId
     * @return
     */
    public int unregisterNode(int nodeId)
    {
        if(nodeId<=0){
            return ReturnCodeKeys.E004;
        }
        if (!nodeList.contains(nodeId))
        {
            return ReturnCodeKeys.E007;
        }
        //获取服务节点状态
        List<TaskInfo> tasksList =taskStatusMap.get(nodeId);
        //如果该服务节点正运行任务，则将运行的任务移到任务挂起队列中，等待调度程序调度
//        if(){
//            
//        }
        nodeList.remove(new Integer(nodeId));
        return ReturnCodeKeys.E006;
    }
    
    /**
     * 添加任务到挂起队列
     * @author zl_wanglei
     * @param taskId
     * @param consumption
     * @return
     */
    public int addTask(int taskId, int consumption)
    {
        if (taskId < 0)
        {
            return ReturnCodeKeys.E009;
        }
        if (taskList.contains(taskId))
        {
            return ReturnCodeKeys.E010;
        }
        taskList.add(taskId);
        taskMap.put(taskId, consumption);
        Collections.sort(taskList, comparatorByTime);
        return ReturnCodeKeys.E008;
    }
    
    /**
     * 删除任务
     * @author zl_wanglei
     * @param taskId
     * @return
     */
    public int deleteTask(int taskId)
    {
        if (!taskList.contains(taskId))
        {
            return ReturnCodeKeys.E012;
        }
        taskList.remove(new Integer(taskId));
        taskMap.remove(new Integer(taskId));
        return ReturnCodeKeys.E011;
    }
    
    
    /**
     * 根据阈值调整任务
     * 
     * @author zl_wanglei
     * @param threshold
     * @return
     */
    public int scheduleTask(int threshold)
    {
        if(threshold <0){
            return ReturnCodeKeys.E002;
        }
        
        
        return ReturnCodeKeys.E014;
    }
    
    /**
     * 查询获得所有已添加任务的任务状态
     * @author zl_wanglei
     * @param tasks
     * @return
     */
    public int queryTaskStatus(List<TaskInfo> tasks)
    {
        for (Integer nodeId : taskStatusMap.keySet())
        {
            tasks.addAll(taskStatusMap.get(nodeId));
        }
        Collections.sort(tasks, comparator);
        System.out.println(tasks);
        return ReturnCodeKeys.E015;
    }
    
}
