/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package policies;

import java.io.IOException;
import java.net.Socket;
import utils.WorkerManager;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;
import javafx.util.Pair;
import utils.HttpComm;

/**
 *
 * @author anantoni
 */
public class RandomSchedulingPolicy implements SchedulingPolicy {
            
        @Override
        public void setWorkerManager() {
            throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
        }

        @Override
        public Socket selectWorker() {
                WorkerManager.getReadLock().lock();
                Map<String,String> workerMap = WorkerManager.getWorkerMap();
                 if (Collections.frequency(workerMap.values(), "OK") == 1) {
                        for (String workerURL : workerMap.keySet()) 
                                if (workerMap.get(workerURL).equals("OK")) {
                                    Pair<String, Integer> hp = HttpComm.splitURL(workerURL);
                                    Socket socket;
                                    try {
                                        socket = new Socket(hp.getKey(), hp.getValue());
                                        return socket;
                                    } catch (IOException ex) {
                                        Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
                                   }
                                }
                }
                else if (Collections.frequency(workerMap.values(), "OK") == 0) {
                        System.err.println("CRITICAL: All workers down - Exiting ");
                        System.exit(-1);
                }
                String workerURL = "";
                do {
                        Random random    = new Random();
                        List<String> keys  = new ArrayList<>(workerMap.keySet());
                        workerURL = keys.get(random.nextInt(keys.size()));    
                        Pair<String, Integer> hp = HttpComm.splitURL(workerURL);
                        Socket socket;
                        try {
                           socket = new Socket(hp.getKey(), hp.getValue());
                           return socket;
                       } catch (IOException ex) {
                           Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
                       }
                } while (workerMap.get(workerURL).equals("DOWN"));
                WorkerManager.getReadLock().unlock();
                
                return null;
        }

    @Override
    public String selectBatchWorker(int size) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }
}
