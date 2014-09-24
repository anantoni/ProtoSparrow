/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package utils;

import com.java.sparrow.protocol.ClientSchedulerProtoc;
import com.java.sparrow.protocol.SchedulerWorkerProtoc;
import com.java.sparrow.protocol.SchedulerWorkerProtoc.HeartBeatResponse;
import java.io.IOException;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;
import policies.PerTaskSamplingSchedulingPolicy;
        
/**
 *
 * @author anantoni
 */
public class HttpComm {
    //static String hostname = "127.0.0.1";
    //static Integer port = new Integer(51000);
    //static final CloseableHttpClient httpclient;
//    static {
//        // Create an HttpClient with the ThreadSafeClientConnManager.
//        // This connection manager must be used if more than one thread will
//        // be using the HttpClient.
//        PoolingHttpClientConnectionManager cm = new PoolingHttpClientConnectionManager();
//        cm.setMaxTotal(100);
//        httpclient = HttpClients.custom().setConnectionManager(cm).build();
//    }
//    
//    public static HttpClient getHttpClient() {
//        return httpclient;
//    }
    
    public static int probe(Socket socket) throws Exception { 
        
        try {
            // write next message type = probe to socket
            ClientSchedulerProtoc.NextMessageType.Builder nextMessageType = ClientSchedulerProtoc.NextMessageType.newBuilder();
            nextMessageType.setType(ClientSchedulerProtoc.NextMessageType.MessageType.TASK);
            // send next message type message
            nextMessageType.build().writeDelimitedTo(socket.getOutputStream());
           
            // receive probe response from worker
            SchedulerWorkerProtoc.ProbeResponse response = SchedulerWorkerProtoc.ProbeResponse.parseDelimitedFrom(socket.getInputStream());
            return response.getLoad();
            
        }   catch (IOException ex) {
            Logger.getLogger(HttpComm.class.getName()).log(Level.SEVERE, null, ex);
        }
        return -1;
    }
    
    public static String heartbeat(String workerURL) {
        String[] pieces = workerURL.split(":");
        String hostname = pieces[0] + pieces[1];
        int port = Integer.parseInt(pieces[2]);
        Socket socket = null;
        try {
            socket = new Socket(hostname, port);
             // write next message type = probe to socket
            ClientSchedulerProtoc.NextMessageType.Builder nextMessageType = ClientSchedulerProtoc.NextMessageType.newBuilder();
            nextMessageType.setType(ClientSchedulerProtoc.NextMessageType.MessageType.HEARTBEAT);
            // send next message type message
            nextMessageType.build().writeDelimitedTo(socket.getOutputStream());
           
            // receive probe response from worker
            HeartBeatResponse response = HeartBeatResponse.parseDelimitedFrom(socket.getInputStream());
            if (response.getStatus() == HeartBeatResponse.StatusType.OK) {
                return "OK";
            } 
            else{ 
                return "DOWN"; 
            }
       } catch (IOException ex) {
           Logger.getLogger(PerTaskSamplingSchedulingPolicy.class.getName()).log(Level.SEVERE, null, ex);
       } finally {
            try {
                socket.close();
            } catch (IOException ex) {
                Logger.getLogger(HttpComm.class.getName()).log(Level.SEVERE, null, ex);
            }
        }
        
        return "";
    }
    
//    public static Map<String, String> multiProbe( List<String> workersList ) throws Exception {
//        Map<String, String> results = new LinkedHashMap<>();
//        for ( String workerURL : workersList ) 
//                results.put(workerURL, probe(workerURL));
//        return results;
//    }
//    
//    public static List<String> lateBindingMultiProbe(List<String> workersList, int jobID) throws Exception {
//        StringBuilder thisSchedulerURL = new StringBuilder("http://");
//        thisSchedulerURL.append(hostname).append(":").append(port).toString();
//        
//        List<String> results = new LinkedList<>();
//        Map<String, String> postArguments = new LinkedHashMap();
//        
//        postArguments.put( "probe", "yes" );
//        postArguments.put( "scheduler-url", thisSchedulerURL.toString() );
//        postArguments.put( "job-id", Integer.toString(jobID) );
//        
//        for ( String workerURL : workersList ) 
//                results.add(workerURL + schedulerPost( workerURL, postArguments ));
//        
//        return results;
//    }
//    
//    public static String sendTask( String workerURL, String jobID, String taskID, String taskCommand ) throws Exception {
//        // TODO: handle worker response for task completion
//        Map<String, String> postArguments = new LinkedHashMap();
//        postArguments.put( "job-id", jobID );
//        postArguments.put( "task-id", taskID);
//        postArguments.put( "task-command", taskCommand );
//        String s = schedulerPost( workerURL, postArguments );
//        return s;
//    }
//    
//    public static String heartbeat( String workerURL ) throws Exception {
//        Map<String, String> postArguments = new LinkedHashMap();
//        postArguments.put( "heartbeat", "yes");
//        String s = schedulerPost( workerURL, postArguments );
//        return s;
//    }
//    
////    public static String schedulerPost( String workerURL, Map<String, String> postArguments) throws Exception {
////            HttpContext context = new BasicHttpContext();
////            HttpPost httpPost = new HttpPost( workerURL );
////            httpPost.setProtocolVersion(HttpVersion.HTTP_1_1);
////            //HttpGet httpGet = new HttpGet(workerURL);
////            // Changing HTTP to 1.1 to avoid delay
////            List <NameValuePair> nvps = new ArrayList <>();
////            postArguments.keySet().stream().forEach((key) -> { 
////                nvps.add( new BasicNameValuePair( key, postArguments.get(key) ) );
////            });
////            httpPost.setEntity(new UrlEncodedFormEntity(nvps));
////            String s = "";
////            try (CloseableHttpResponse response2 = httpclient.execute(httpPost, context)) {
////                HttpEntity entity2 = response2.getEntity();
////                s = EntityUtils.toString(entity2);
////                EntityUtils.consume(entity2);
////            }
////            finally {
////                httpPost.releaseConnection();
////            }
////            return s;
////        
////    }
//    
//    public static String schedulerPost( String workerURL, Map<String, String> postArguments) throws Exception {
//        try (CloseableHttpClient httpclient = HttpClients.createDefault()) {
//            
//                HttpPost httpPost = new HttpPost( workerURL );
//                httpPost.setProtocolVersion(HttpVersion.HTTP_1_1);
//                List <NameValuePair> nvps = new ArrayList <>();
//                postArguments.keySet().stream().forEach((key) -> { 
//                    nvps.add( new BasicNameValuePair( key, postArguments.get(key) ) );
//                });
//                //for ( String key : postArguments.keySet() ) 
//                //nvps.add( new BasicNameValuePair( key, postArguments.get(key) ) );
//
//                //nvps.add(new BasicNameValuePair("username", "vip"));
//                //nvps.add(new BasicNameValuePair("password", "secret"));
//                httpPost.setEntity(new UrlEncodedFormEntity(nvps));
//
//                try (CloseableHttpResponse response2 = httpclient.execute(httpPost)) {
//                        System.out.println(response2.getStatusLine());
//                        HttpEntity entity2 = response2.getEntity();
//                        String s = EntityUtils.toString(entity2);
//        //                byte[] entityContent = EntityUtils.toByteArray(entity2);
//        //                String a = new String(entityContent);
//        //                System.out.println(a);
//                        // do something useful with the response body
//                        // and ensure it is fully consumed
//                        EntityUtils.consume(entity2);
//                        return s;
//                }
//        }
//    }
}
