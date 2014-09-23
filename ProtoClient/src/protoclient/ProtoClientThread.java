/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package protoclient;

import com.java.sparrow.protocol.ClientSchedulerProtoc;
import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Random;
import com.java.sparrow.protocol.ClientSchedulerProtoc.JobBatch;
import com.java.sparrow.protocol.ClientSchedulerProtoc.NextMessageType;
import com.java.sparrow.protocol.ClientSchedulerProtoc.SchedulerResponse;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.AtomicCounter;
/**
 *
 * @author anantoni
 */
public class ProtoClientThread implements Runnable{
    private final String schedulerHostname;
    private final int schedulerPort;
    private Socket socket;

    /*constructor*/
    public ProtoClientThread(String hostname, int port) {
        this.schedulerHostname = hostname;
        this.schedulerPort = port;
    }

    /*interface methods*/
    @Override
    public void run() {
    // connect to server
        try {
            this.socket = new Socket(this.schedulerHostname, this.schedulerPort);
            System.out.println("Connected with scheduler " + this.socket.getInetAddress() +
                                                                                           ":" + this.socket.getPort());
        }
        catch (UnknownHostException e) {
            System.out.println(e);
            System.exit(-1);
        }
        catch (IOException e) {
            System.out.println(e);
            System.exit(-1);
        }

        //creating jobs for http requests to scheduler

        //set number of jobs
        int numOfJobs = 5;

        for(int j=0; j< numOfJobs; j++){
            
            // build job batch message
            NextMessageType.Builder nextMessageType = NextMessageType.newBuilder();
            nextMessageType.setType(NextMessageType.MessageType.JOB_BATCH);
            try {
                nextMessageType.build().writeDelimitedTo(socket.getOutputStream());
            } catch (IOException ex) {
                Logger.getLogger(ProtoClientThread.class.getName()).log(Level.SEVERE, null, ex);
            }
            JobBatch.Builder jobBatch = JobBatch.newBuilder();
            jobBatch.setTimesToExecute(numOfJobs);
            int jobSelection = j%2;
            if (jobSelection == 0)
                    jobBatch.setTaskCommand("task1.sh");
            else
                    jobBatch.setTaskCommand("task2.sh");
            jobBatch.setTaskNumber(10);
            
            // write job batch message to socket
            try {
                jobBatch.build().writeDelimitedTo (socket.getOutputStream());
            } catch (IOException ex) {
                Logger.getLogger(ProtoClientThread.class.getName()).log(Level.SEVERE, null, ex);
            }
            
            // receive response from scheduler
            try {
                SchedulerResponse response = SchedulerResponse.parseDelimitedFrom(socket.getInputStream());
                if (response.getStatus() == SchedulerResponse.StatusType.OK) {
                    System.out.println("Scheduler received job batch successfully");
                }
                else {
                    System.out.println("Job batch not sent successfully");
                }
            } catch (IOException ex) {
                Logger.getLogger(ProtoClientThread.class.getName()).log(Level.SEVERE, null, ex);
            }
        }

        //closing socket...
        try {
            socket.close();
        }
        catch (IOException e) {
            System.out.println(e);
        }
    }

   private int randInt(int min, int max) {
        Random rand = new Random();
        return  rand.nextInt((max - min) + 1) + min;
    }
}