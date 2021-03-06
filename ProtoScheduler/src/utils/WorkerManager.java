/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package utils;

import java.net.SocketException;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Level;
import java.util.logging.Logger;
/**
 *
 * @author anantoni
 */
public class WorkerManager {
    private static Map<String, String> workerMap;  
    private static final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private static final Lock read  = readWriteLock.readLock();
    private static final Lock write = readWriteLock.writeLock();
    
    public WorkerManager(List<String> workerList) {
        workerMap = new LinkedHashMap<>();
        write.lock();
        for (String workerURL :workerList) {
            String result = HttpComm.heartbeat(workerURL);
            if (result.equals("OK"))
                    workerMap.put(workerURL, "OK");
            else
                    workerMap.put(workerURL, "DOWN");
        }
        write.unlock();
    }
    
    public static void useWorkerList(List<String> workerList) {
        workerMap = new LinkedHashMap<>();
        write.lock();
        for (String workerURL :workerList) {
            String result = HttpComm.heartbeat(workerURL);
            if (result.equals("OK"))
                    workerMap.put(workerURL, "OK");
            else
                    workerMap.put(workerURL, "DOWN");
        }
        write.unlock();
    }
    
    public static void printWorkerMap() {
        Set<String> keys = workerMap.keySet();
        for (String key : keys) {
                System.out.println(key);
        }

        Collection<String> values = workerMap.values();
        for (String value : values) {
                System.out.println(value);
        }
    }
    
    public static String getWorkerStatus(String workerURL) {
        return workerMap.get(workerURL);
    }
    
    public static void updateWorkerStatus(){
        write.lock();
        for (String workerURL : workerMap.keySet()) {
            String result = HttpComm.heartbeat(workerURL);
            if (result.equals("OK"))
                workerMap.put(workerURL, "OK");
            else
                workerMap.put(workerURL, "DOWN");
        }
        write.unlock();
    }
    
    public static Map<String,String> getWorkerMap() {
        return workerMap;
    }
    
    public static Lock getReadLock() {
        return read;
    }
    
     public static Lock getWriteLock() {
        return write;
    }
    
    public static int getWorkerNumber() {
        return workerMap.keySet().size();
    }
    
}
