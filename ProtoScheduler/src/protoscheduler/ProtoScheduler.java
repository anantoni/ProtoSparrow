/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package protoscheduler;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import utils.WorkerManager;

/**
 *
 * @author anantoni
 */
public class ProtoScheduler {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     */
    
    public static void main(String[] args) throws IOException {
        ExecutorService executorService = Executors.newFixedThreadPool(100);
        ServerSocket serverSocket = new ServerSocket(Integer.parseInt(args[0]));
        
        System.out.println("Listening on port: " + args[0]);
        
        int fixedExecutorSize = 8;
         //Creating fixed size executor
        ThreadPoolExecutor taskCommExecutor = new ThreadPoolExecutor(fixedExecutorSize, 
                                                                     fixedExecutorSize, 0L, 
                                                                     TimeUnit.MILLISECONDS, 
                                                                     new LinkedBlockingQueue<Runnable>());
//        //Creating fixed size executor
//        ThreadPoolExecutor taskExecutor = new ThreadPoolExecutor( fixedExecutorSize, 
//                                                                                                                      fixedExecutorSize, 0L, 
//                                                                                                                      TimeUnit.MILLISECONDS,
//                                                                                                                      new LinkedBlockingQueue<>());
        
        ArrayList<String> workerList = new ArrayList<>();
            
        // Read list of workers from configuration file
        try(BufferedReader br = new BufferedReader(new FileReader( "./config/workers.conf" ))) {
                for(String line; (line = br.readLine()) != null; ) {
                        workerList.add(line);
                }
        } catch (FileNotFoundException ex) {
                Logger.getLogger(ProtoScheduler.class.getName()).log(Level.SEVERE, null, ex);
        } catch (IOException ex) {
                Logger.getLogger(ProtoScheduler.class.getName()).log(Level.SEVERE, null, ex);
        }

        // Initialize worker manager
        try {
                WorkerManager.useWorkerList(workerList);
        } catch (Exception ex) {
                Logger.getLogger(ProtoScheduler.class.getName()).log(Level.SEVERE, null, ex);
                System.exit(-1);
        }
        WorkerManager.printWorkerMap();
        
        while(true) {
             Socket socket = serverSocket.accept();
             socket.setSoTimeout(3000);
             // process client connection..
             executorService.execute(new GenericConnectionHandler(socket, taskCommExecutor));
        }
    }
}
