/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package protoscheduler;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import com.java.sparrow.protocol.ClientSchedulerProtoc.JobBatch;
import com.java.sparrow.protocol.ClientSchedulerProtoc.NextMessageType;
import com.java.sparrow.protocol.ClientSchedulerProtoc.SchedulerResponse;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;
import policies.PerTaskSamplingSchedulingPolicy;
import policies.RandomSchedulingPolicy;
import policies.SchedulingPolicy;
import utils.Task;
/**
 *
 * @author anantoni
 */
public class GenericConnectionHandler implements Runnable{

    private final Socket socket;
    private final ThreadPoolExecutor taskCommExecutor;
 
    public GenericConnectionHandler(Socket socket, ThreadPoolExecutor taskCommExecutor) {
        this.socket = socket;
        this.taskCommExecutor = taskCommExecutor;
    }
 
    @Override
    public void run() {
         //BufferedReader reader = null;
         //PrintWriter writer = null;
         
         try {
            //reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            //writer = new PrintWriter(socket.getOutputStream(), true);
            NextMessageType expectedType = NextMessageType.parseDelimitedFrom(socket.getInputStream());
            
            // if probe response from worker
            if (expectedType.getType() == NextMessageType.MessageType.PROBE_RESPONSE) {
                
            }
            // else if other response from worker
            else if (expectedType.getType() == NextMessageType.MessageType.WORKER_RESPONSE) {
                
            }
            // else if new job batch
            else if (expectedType.getType() == NextMessageType.MessageType.JOB_BATCH) {
                JobBatch job = JobBatch.parseDelimitedFrom(socket.getInputStream());
                System.out.println( job.getTimesToExecute() + " " + 
                                                 job.getTaskCommand() + " " + 
                                                 job.getTaskNumber());
                
                // process request
                
                // send OK message to client
                SchedulerResponse.Builder schedulerResponse = SchedulerResponse.newBuilder();
                schedulerResponse.setStatus(SchedulerResponse.StatusType.OK);
                try {
                    schedulerResponse.build().writeDelimitedTo(socket.getOutputStream());
                } catch (IOException ex) {
                    Logger.getLogger(GenericConnectionHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
                
                // set policy and select worker to send task to
                SchedulingPolicy policy = new RandomSchedulingPolicy();
                //SchedulingPolicy policy = new PerTaskSamplingSchedulingPolicy();
                for (int i = 0; i < job.getTimesToExecute() ; i++)
                     for (int j = 0; j < job.getTaskNumber() ; j++) {
                         Pair<String, Integer> hp = policy.selectWorker();
                         Socket workerSocket = new Socket(hp.getKey(), hp.getValue());
                         Task task = new Task(i, j, job.getTaskCommand());
                         taskCommExecutor.execute(new TaskCommThread(task, workerSocket));
                     }
            }                
        } catch (SocketTimeoutException e) {
            System.out.println("Connection timed out");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
               // if(reader != null) reader.close();
                //if(writer != null) writer.close();
                socket.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
}
