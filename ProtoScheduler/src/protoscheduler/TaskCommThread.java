/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package protoscheduler;

import com.java.sparrow.protocol.ClientSchedulerProtoc;
import com.java.sparrow.protocol.SchedulerWorkerProtoc;
import java.io.IOException;
import java.net.Socket;
import utils.Task;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author anantoni
 */
public class TaskCommThread implements Runnable{

    private final Task task;
    private final Socket socket;
    //private final SchedulingPolicy policy, backupPolicy;

    TaskCommThread(Task task, Socket socket) {
        super();
        this.task = task;
        this.socket = socket;
        //this.policy = policy;
        //this.backupPolicy = new PerTaskSamplingSchedulingPolicy();
    }
    
    
    @Override
    public void run() {
        //boolean workerDown = false;
        //String workerURL = policy.selectWorker();
        //System.out.println(workerURL);
//        Date dNow = new Date( );
//        SimpleDateFormat ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
//        StatsLog.writeToLog(ft.format(dNow) + " Thread #" + Thread.currentThread().getId() + " started");

        try {
            // write next message type = task to socket
            ClientSchedulerProtoc.NextMessageType.Builder nextMessageType = ClientSchedulerProtoc.NextMessageType.newBuilder();
            nextMessageType.setType(ClientSchedulerProtoc.NextMessageType.MessageType.TASK);
            // send next message type message
            nextMessageType.build().writeDelimitedTo(socket.getOutputStream());

            // build task message
            SchedulerWorkerProtoc.Task.Builder taskMessage = SchedulerWorkerProtoc.Task.newBuilder();
            taskMessage.setJobId(task.getJobID());
            taskMessage.setTaskId(task.getTaskID());
            taskMessage.setTaskCommand(task.getCommand());
            
            // send task message
            taskMessage.build().writeDelimitedTo(socket.getOutputStream());
        }   catch (IOException ex) {
            Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        
//        try {
//            task.setResult(HttpComm.sendTask(workerURL, String.valueOf(task.getJobID()), String.valueOf(task.getTaskID()), task.getCommand()));
//            dNow = new Date( );
//            ft = new SimpleDateFormat ("E yyyy.MM.dd 'at' hh:mm:ss a zzz");
//            StatsLog.writeToLog(ft.format(dNow) + " Thread #" + Thread.currentThread().getId() + " Sending job #" + task.getJobID() + " task #" + task.getTaskID() + " to worker: " + workerURL);
//        } catch ( HttpHostConnectException | NoHttpResponseException ex) {
//            WorkerManager.getWriteLock().lock();
//            WorkerManager.getWorkerMap().put(workerURL, "DOWN");
//            WorkerManager.getWriteLock().unlock();
//            Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
//            //workerDown = true;
//        } 
//        catch ( SocketException ex) {
//            WorkerManager.getWriteLock().lock();
//            WorkerManager.getWorkerMap().put(workerURL, "DOWN");
//            WorkerManager.getWriteLock().unlock();
//            Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
//            //workerDown = true;
//        } catch (Exception ex) {
//            Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
//        }
            
            // This part is executed only if the worker selected by the primary policy goes down
            // It is guaranteed that as long as worker is up the task will be completed eventually
//        while (workerDown == true) {
//            try {
//                    workerURL = backupPolicy.selectWorker();
//                    task.setResult(HttpComm.sendTask(workerURL, String.valueOf(task.getJobID()), String.valueOf( task.getTaskID() ), task.getCommand()));
//                    workerDown = false;
//            } catch (Exception ex) {
//                    Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
            
            // This part is executed only if the worker selected by the primary policy goes down
            // It is guaranteed that as long as worker is up the task will be completed eventually
//        while (workerDown == true) {
//            try {
//                    workerURL = backupPolicy.selectWorker();
//                    task.setResult(HttpComm.sendTask(workerURL, String.valueOf(task.getJobID()), String.valueOf( task.getTaskID() ), task.getCommand()));
//                    workerDown = false;
//            } catch (Exception ex) {
//                    Logger.getLogger(TaskCommThread.class.getName()).log(Level.SEVERE, null, ex);
//            }
//        }
        
    
    }
};

