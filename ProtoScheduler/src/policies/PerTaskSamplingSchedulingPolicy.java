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
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;
import utils.RandomGenerator;

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
    public Pair<String, Integer> selectWorker() {
        WorkerManager.getReadLock().lock();
        Map<String,String> workerMap = WorkerManager.getWorkerMap();        
        List<String> keys  = new ArrayList<>(workerMap.keySet());
        
        if (Collections.frequency(workerMap.values(), "OK") == 1) {
            for (String workerURL : workerMap.keySet()) 
                    if (workerMap.get(workerURL).equals("OK")) {
                         Pair<String, Integer> hp = HttpComm.splitURL(workerURL);
                         WorkerManager.getReadLock().unlock();
                         return hp;
                    }
        }
        else if (Collections.frequency(workerMap.values(), "OK") == 0) {
            System.err.println("CRITICAL: All workers down - Exiting ");
            System.exit(-1);
        }
        // Select a random active worker
        String workerURL = "";
        do {
                workerURL = keys.get( RandomGenerator.getRandomGenerator().nextInt(keys.size()) );           
        } while (workerMap.get(workerURL).equals("DOWN"));
        
        // Select a second random active worker, different from the first one
        String workerURL1 = "";
        do {
                workerURL1 = keys.get( RandomGenerator.getRandomGenerator().nextInt(keys.size()) );           
        } while (workerMap.get(workerURL1).equals("DOWN") || workerURL.equals(workerURL1));
        
        WorkerManager.getReadLock().unlock();
        
        Pair<String, Integer> hp = HttpComm.splitURL(workerURL);
        int result = -1;
        Socket socket = null;
        try {
            socket = new Socket(hp.getKey(), hp.getValue());
            result = HttpComm.probe(socket);
            System.out.println("Worker: " + hp.getKey() + " port: " + hp.getValue() + " probe result: " + result);
        } catch (IOException ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        Pair<String, Integer> hp1 = HttpComm.splitURL(workerURL1);
        int result1 = -1;
        Socket socket1 = null;
        try {
            socket1 = new Socket(hp1.getKey(), hp1.getValue());
            result1 = HttpComm.probe(socket1);
            System.out.println("Worker: " + hp1.getKey() + " port: " + hp1.getValue() + " probe result: " + result1);
        } catch (IOException ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        } catch (Exception ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }       
        
        try {
            socket.close();
            socket1.close();
        } catch (IOException ex) {
            Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
        }
        return result > result1? hp1 : hp;
    }   

    @Override
    public String selectBatchWorker(int size) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
