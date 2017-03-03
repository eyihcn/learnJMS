package eyihcn.activemq.pubsub;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

public class JMSSubscriber {
	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;// 默认连接用户名
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;// 默认连接密码
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;// 默认连接地址
	private static final int TOPIC_COUNT = 5;// 主题数量

	public static void main(String[] args) {
		ConnectionFactory connectionFactory;// 连接工厂
		Connection connection = null;// 连接

		Session session;// 会话 接受或者发送消息的线程
		Topic[] topics;// 消息的目的地

		MessageConsumer[] messageConsumers;// 消息的消费者

		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSSubscriber.USERNAME, JMSSubscriber.PASSWORD,
				JMSSubscriber.BROKEURL);

		try {
			// 通过连接工厂获取连接
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建session
			session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
			topics = new Topic[JMSSubscriber.TOPIC_COUNT];
			// 创建创建topics
			for (int i = 0; i < JMSSubscriber.TOPIC_COUNT; i++) {
				topics[i] = session.createTopic("topic-" + i);
			}
			// 异步的方式 ，处理已经订阅主题的消息
			Listener[] listeners = new Listener[JMSSubscriber.TOPIC_COUNT];
			for (int i = 0; i < JMSSubscriber.TOPIC_COUNT; i++) {
				listeners[i] = new Listener();
			}

			messageConsumers = new MessageConsumer[JMSSubscriber.TOPIC_COUNT];
			// 创建创建消费者
			for (int i = 0; i < JMSSubscriber.TOPIC_COUNT; i++) {
				messageConsumers[i] = session.createConsumer(topics[i]);
				listeners[i].setTopic(topics[i]);
				listeners[i].setConsumerIndex(i);
				// 通过异步的方式处理接受的消息
				messageConsumers[i].setMessageListener(listeners[i]);
			}

			// 创建消息消费者
			// int i = 0;
			// while (true) {
			// if (++i == JMSSubscriber.TOPIC_COUNT) {
			// i = 0;
			// }
			// }

		} catch (JMSException e) {
			e.printStackTrace();
		}
	}
}

class Listener implements MessageListener {

	private int consumerIndex;
	private Topic topic;
	private MessageConsumer messageConsumer;

	public Listener() {
	}

	public Listener(int consumerIndex, Topic topic, MessageConsumer messageConsumer) {
		super();
		this.consumerIndex = consumerIndex;
		this.topic = topic;
		this.messageConsumer = messageConsumer;
	}

	public void onMessage(Message message) {
		try {
			TimeUnit.SECONDS.sleep(1);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		ActiveMQMapMessage map = (ActiveMQMapMessage) message;

		try {
			System.out.println("messageConsumer-" + consumerIndex + " 消费了" + topic.getTopicName() + " 的消息 :"
					+ map.getContentMap());
		} catch (JMSException e) {
			e.printStackTrace();
		}
	}

	public int getConsumerIndex() {
		return consumerIndex;
	}

	public void setConsumerIndex(int consumerIndex) {
		this.consumerIndex = consumerIndex;
	}

	public Topic getTopic() {
		return topic;
	}

	public void setTopic(Topic topic) {
		this.topic = topic;
	}

	public MessageConsumer getMessageConsumer() {
		return messageConsumer;
	}

	public void setMessageConsumer(MessageConsumer messageConsumer) {
		this.messageConsumer = messageConsumer;
	}

}
