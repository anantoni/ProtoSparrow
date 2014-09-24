/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package policies;

import java.net.Socket;

/**
 *
 * @author anantoni
 */
public interface SchedulingPolicy {
    public void setWorkerManager();
    public Socket selectWorker();
    public String selectBatchWorker(int size);
}
