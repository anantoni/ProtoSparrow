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
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author anantoni
 */
public class GenericConnectionHandler implements Runnable{

    private final Socket socket;
 
    public GenericConnectionHandler(Socket socket) {
        this.socket = socket;
    }
 
    @Override
    public void run() {
         BufferedReader reader = null;
         PrintWriter writer = null;
         
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
                System.out.println(job.getTimesToExecute() + " " + 
                                                job.getTaskCommand() + " " + job.getTaskNumber());
                
                // process request
                
                // send OK message to client
                SchedulerResponse.Builder schedulerResponse = SchedulerResponse.newBuilder();
                schedulerResponse.setStatus(SchedulerResponse.StatusType.OK);
                try {
                    schedulerResponse.build().writeDelimitedTo(socket.getOutputStream());
                } catch (IOException ex) {
                    Logger.getLogger(GenericConnectionHandler.class.getName()).log(Level.SEVERE, null, ex);
                }
            }                
        } catch (SocketTimeoutException e) {
            System.out.println("Connection timed out");
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            try {
                if(reader != null) reader.close();
                if(writer != null) writer.close();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }
    
}
