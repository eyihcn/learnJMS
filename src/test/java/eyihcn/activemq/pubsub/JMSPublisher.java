package eyihcn.activemq.pubsub;

import java.util.concurrent.TimeUnit;

import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MapMessage;
import javax.jms.MessageProducer;
import javax.jms.Session;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQMapMessage;

public class JMSPublisher {

	private static final String USERNAME = ActiveMQConnection.DEFAULT_USER;// 默认连接用户名
	private static final String PASSWORD = ActiveMQConnection.DEFAULT_PASSWORD;// 默认连接密码
	private static final String BROKEURL = ActiveMQConnection.DEFAULT_BROKER_URL;// 默认连接地址

	private static final int TOPIC_COUNT = 5;// 主题数量

	public static void main(String[] args) {
		ConnectionFactory connectionFactory;// 连接工厂
		Connection connection = null;// 连接

		Session session;// 会话 接受或者发送消息的线程
		Topic[] topics;// 多个Topic

		MessageProducer messageProducer;// 消息生产者

		// 实例化连接工厂
		connectionFactory = new ActiveMQConnectionFactory(JMSPublisher.USERNAME, JMSPublisher.PASSWORD,
				JMSPublisher.BROKEURL);
		try {
			connection = connectionFactory.createConnection();
			// 启动连接
			connection.start();
			// 创建 会话：第一个boolean参数指定是否是 transacted，
			session = connection.createSession(true, Session.AUTO_ACKNOWLEDGE);
			messageProducer = session.createProducer(null); // 创建生产者，先不指定Destination
			topics = new Topic[JMSPublisher.TOPIC_COUNT];
			// 创建主题
			for (int i = 0; i < JMSPublisher.TOPIC_COUNT; i++) {
				topics[i] = session.createTopic("topic-" + i);
			}

			// 发送消息
			int i = 0;
			for (;;) {
				MapMessage mapMsg = session.createMapMessage();
				mapMsg.setString("msg-name", "生产的第" + i + "条消息");
				mapMsg.setInt("num", i);
				messageProducer.send(topics[i], mapMsg);
				System.out.println("生产者：messageProducer,将消息：" + ((ActiveMQMapMessage) mapMsg).getContentMap()
						+ " 发布到主题：" + topics[i].getTopicName());
				if (++i == JMSPublisher.TOPIC_COUNT) {
					i = 0;
					if (session.getTransacted()) {
						session.commit();
					}
					System.out.println("session.commit();");
				}
				TimeUnit.SECONDS.sleep(1);
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (null != connection) {
				try {
					connection.close();
				} catch (JMSException e) {
					e.printStackTrace();
				}
			}
		}

	}

}
