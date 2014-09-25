/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package protoclient;

import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Properties;
import javafx.util.Pair;
import java.io.IOException;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
/**
 *
 * @author anantoni
 */
public class ProtoClient {
    private static ArrayList<Pair<String, String>> schedulers = new ArrayList<>();

    /**
     * @param args the command line arguments
     */
    public static void main(String[] args) {
        loadSchedulers();
        ExecutorService executor = Executors.newFixedThreadPool(4);

        for (int i = 0; i < 400; i++) {
            Pair<String, Integer> chosenScheduler = chooseScheduler();
            Runnable worker = new ProtoClientThread( chosenScheduler.getKey(),
                                                                                         chosenScheduler.getValue(),
                                                                                         i);
            executor.execute(worker);
        }
        executor.shutdown();
        while (!executor.isTerminated()) {}
  }
    
  private static ArrayList<Pair<String, String>> loadSchedulers(){        
        Properties prop = new Properties();
        InputStream input = null;

        try {
            // open and load available schedulers property file
            input = new FileInputStream("./config/available_schedulers.properties");
            prop.load(input);

            // get the property value and print it out
            int nos = Integer.parseInt(prop.getProperty("numberOfSchedulers"));
            schedulers.add(new Pair<>( prop.getProperty("scheduler1.hostname"), 
                                                           prop.getProperty("scheduler1.port")));
            
            //schedulers.add(new Pair<>( prop.getProperty("scheduler2.hostname"), 
            //prop.getProperty("scheduler2.port")));
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            if (input != null) {
                try {
                        input.close();
                } catch (IOException e) {
                        e.printStackTrace();
                }   
            }
        }
        return schedulers;
    }
  
    private static Pair<String, Integer> chooseScheduler(){   
        Collections.shuffle(schedulers);
        return new Pair<>(schedulers.get(0).getKey(), Integer.parseInt(schedulers.get(0).getValue()));
    }
    
}
