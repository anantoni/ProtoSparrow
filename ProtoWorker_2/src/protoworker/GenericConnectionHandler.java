/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package protoworker;

import com.java.sparrow.protocol.ClientSchedulerProtoc;
import com.java.sparrow.protocol.ClientSchedulerProtoc.NextMessageType;
import com.java.sparrow.protocol.SchedulerWorkerProtoc;
import com.java.sparrow.protocol.SchedulerWorkerProtoc.HeartBeatResponse;
import com.java.sparrow.protocol.SchedulerWorkerProtoc.ProbeResponse;
import com.java.sparrow.protocol.SchedulerWorkerProtoc.TaskMessage;
import com.java.sparrow.protocol.SchedulerWorkerProtoc.WorkerResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.concurrent.ThreadPoolExecutor;

/**
 *
 * @author anantoni
 */
class GenericConnectionHandler implements Runnable {
    private final Socket socket;
    private final ThreadPoolExecutor taskExecutor;
    
    public GenericConnectionHandler(Socket socket, ThreadPoolExecutor taskExecutor) {
        this.socket = socket;
        this.taskExecutor = taskExecutor;
    }

    @Override
    public void run() {
        BufferedReader reader = null;
         PrintWriter writer = null;
         
         try {
            NextMessageType expectedType = NextMessageType.parseDelimitedFrom(socket.getInputStream());
            
            // if probe from scheduler
            if (expectedType.getType() == NextMessageType.MessageType.PROBE) {
                ProbeResponse.Builder response = ProbeResponse.newBuilder();
                
                response.setLoad(taskExecutor.getPoolSize() + taskExecutor.getActiveCount());
                // send probe respnse aka "load"
                response.build().writeTo(socket.getOutputStream());
            }
            // else if heartbeat
            else if (expectedType.getType() == NextMessageType.MessageType.HEARTBEAT) {
                HeartBeatResponse.Builder response = HeartBeatResponse.newBuilder();
                response.setStatus(HeartBeatResponse.StatusType.OK);
                
                // send OK to scheduler
                response.build().writeDelimitedTo(socket.getOutputStream());
            }
            // else if task to execute
            else if (expectedType.getType() == NextMessageType.MessageType.TASK) {
                TaskMessage task = TaskMessage.parseDelimitedFrom(socket.getInputStream());
                
                // process request
                Runnable t = new TaskExecutorThread(task.getJobId(), 
                                                    task.getTaskId(), 
                                                    task.getTaskCommand());
                taskExecutor.execute(t);
                
                // build worker response
                WorkerResponse.Builder workerResponse = WorkerResponse.newBuilder();
                workerResponse.setStatus(WorkerResponse.StatusType.OK);
                    
                // send OK message to client
                workerResponse.build().writeDelimitedTo(socket.getOutputStream());
            }                
        } catch (SocketTimeoutException e) {
            System.out.println("Connection timed out");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(reader != null) reader.close();
                if(writer != null) writer.close();
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
