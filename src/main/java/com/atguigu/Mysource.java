package com.atguigu;

import org.apache.flume.Context;
import org.apache.flume.EventDeliveryException;
import org.apache.flume.PollableSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.event.SimpleEvent;
import org.apache.flume.source.AbstractSource;

import java.util.HashMap;

public class Mysource extends AbstractSource implements Configurable,PollableSource {

    private String head;
    private Long sleepTime;

    /**
     * 用于获取配置文件中参数
     * @param context
     */
    @Override
    public void configure(Context context) {
        head = context.getString("preHead");
        sleepTime = context.getLong("sleepTime", 1000l);

    }

    /**
     * 真正执行代码的地方
     * @return
     * @throws EventDeliveryException
     */
    @Override
    public Status process() throws EventDeliveryException {
        try {
            //创建event对象
            SimpleEvent event = new SimpleEvent();
            //封装event的信息头及信息体
            event.setHeaders(new HashMap<>());
            event.setBody( (head +",hello world").getBytes());
            //将event写出去
            getChannelProcessor().processEvent(event);
            //设置睡眠3秒
            Thread.sleep(sleepTime);
        } catch (InterruptedException e) {
            e.printStackTrace();
            return Status.BACKOFF;
        }

        return Status.READY;
    }

    @Override
    public long getBackOffSleepIncrement() {
        return 0;
    }

    @Override
    public long getMaxBackOffSleepInterval() {
        return 0;
    }


}
