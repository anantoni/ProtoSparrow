/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package policies;

import java.io.IOException;
import java.net.Socket;
import utils.HttpComm;
import utils.WorkerManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author anantoni
 */
public class PerTaskSamplingSchedulingPolicy implements SchedulingPolicy {

    @Override
    public void setWorkerManager() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    @Override
    public Socket selectWorker() {
        WorkerManager.getReadLock().lock();
        Map<String,String> workerMap = WorkerManager.getWorkerMap();        
        Random random    = new Random();
        List<String> keys  = new ArrayList<>(workerMap.keySet());
        
        if (Collections.frequency(workerMap.values(), "OK") == 1) {
            for (String workerURL : workerMap.keySet()) 
                    if (workerMap.get(workerURL).equals("OK")) {
                         String[] pieces = workerURL.split(":");
                         String hostname = pieces[0] + pieces[1];
                         int port = Integer.parseInt(pieces[2]);
                         Socket socket;
                        try {
                            socket = new Socket(hostname, port);
                        } catch (IOException ex) {
                            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
                        }
                    }
        }
        else if (Collections.frequency(workerMap.values(), "OK") == 0) {
            System.err.println("CRITICAL: All workers down - Exiting ");
            System.exit(-1);
        }
        // Select a random active worker
        String workerURL = "";
        do {
                workerURL = keys.get( random.nextInt(keys.size()) );           
        } while (workerMap.get(workerURL).equals("DOWN"));
        
        // Select a second random active worker, different from the first one
        String workerURL1 = "";
        do {
                workerURL1 = keys.get( random.nextInt(keys.size()) );           
        } while (workerMap.get(workerURL1).equals("DOWN") || workerURL.equals(workerURL1));
        
        WorkerManager.getReadLock().unlock();
        
        String[] pieces = workerURL.split(":");
        String hostname = pieces[0] + pieces[1];
        int port = Integer.parseInt(pieces[2]);
        int result = -1;
        Socket socket = null;
        try {
            socket = new Socket(hostname, port);
            result = HttpComm.probe(socket);
        } catch (IOException ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            
            //System.out.println("Worker " + workerURL + ": " + result);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        pieces = workerURL.split(":");
        hostname = pieces[0] + pieces[1];
        port = Integer.parseInt(pieces[2]);
        int result1 = -1;
        Socket socket1 = null;
        try {
            socket1 = new Socket(hostname, port);
            result = HttpComm.probe(socket1);
        } catch (IOException ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        try {
            
            //System.out.println("Worker " + workerURL + ": " + result);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        if (result1 > result) {
            try {
                socket.close();
            } catch (IOException ex) {
                Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
            }
            return socket1;
        }
        else {
            try {
                socket1.close();
            } catch (IOException ex) {
                Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
            }
            return socket;
        }
    }   

    @Override
    public String selectBatchWorker(int size) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
