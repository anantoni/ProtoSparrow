/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package protoworker;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author anantoni
 */
public class ProtoWorker {

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
        ThreadPoolExecutor taskExecutor = new ThreadPoolExecutor( fixedExecutorSize, 
                                                                                                                      fixedExecutorSize, 0L, 
                                                                                                                      TimeUnit.MILLISECONDS,
                                                                                                                      new LinkedBlockingQueue<>());
        while(true) {
             Socket socket = serverSocket.accept();
             socket.setSoTimeout(3000);
             // process client connection..
             executorService.execute(new GenericConnectionHandler(socket, taskExecutor));
        }
    }
}
