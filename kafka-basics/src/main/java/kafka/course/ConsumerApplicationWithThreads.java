package kafka.course;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;

public class ConsumerApplicationWithThreads {
    private static Logger logger = LoggerFactory.getLogger(ConsumerApplicationWithThreads.class);

    public static void main(String[] args) {
           String bootstrapServer = "localhost:9092";
           String topic = "com.mytopic.metric";
           String groupId = "myapp-first";

        CountDownLatch latch = new CountDownLatch(1);

           Runnable consumerThread = new ConsumerThread(
                   latch,
                   topic,
                   bootstrapServer,
                   groupId
           );
           Thread thread = new Thread(consumerThread);
           thread.start();

           // shutdown hook
            Runtime.getRuntime().addShutdownHook(new Thread(
                    ()-> {
                        logger.info("caught shutdown hook");
                        ((ConsumerThread) consumerThread).shutdown();
                        try{
                            latch.await();
                        }catch (InterruptedException e){
                           e.printStackTrace();
                        }
                    }
            ));

           try {
               latch.wait();
           }catch (InterruptedException ex){
               ex.printStackTrace();
           }finally {
               logger.info("Applicaiton Consumer closed.");
           }
    }

}
