package ee.ttu.idu0080.raamatupood.client;

import java.math.BigDecimal;
import java.util.Date;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import ee.ttu.idu0080.raamatupood.server.EmbeddedBroker;
import ee.ttu.idu0080.raamatupood.types.Tellimus;
import ee.ttu.idu0080.raamatupood.types.TellimuseRida;
import ee.ttu.idu0080.raamatupood.types.Toode;

/**
 * JMS sõnumite tarbija. ühendub broker-i urlile
 * 
 * @author Allar Tammik
 * @date 08.03.2010
 */
/**
 * JMS sõnumite tootja. Ühendub brokeri url-ile
 * 
 * @author Allar Tammik
 * @date 08.03.2010
 */
public class Consumer {
	private static final Logger log = Logger.getLogger(Consumer.class);
	public static final String SUBJECT = "tellimuse.edastamine"; // järjekorra nimi
	public static final String SUBJECT_ANSWER = "tellimuse.vastus";
	

	private String user = ActiveMQConnection.DEFAULT_USER;// brokeri jaoks vaja
	private String password = ActiveMQConnection.DEFAULT_PASSWORD;

	long sleepTime = 1000; // 1000ms

	private int messageCount = 1;
	private long timeToLive = 1000000;
	private String url = EmbeddedBroker.URL;

	public static void main(String[] args) {
		Consumer tellijaTool = new Consumer();
		tellijaTool.run();
	}

	public void run() {
		Connection connection = null;
		try {
			log.info("Connecting to URL: " + url);
			log.debug("Sleeping between publish " + sleepTime + " ms");
			if (timeToLive != 0) {
				log.debug("Messages time to live " + timeToLive + " ms");
			}

			// 1. Loome ühenduse
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
					user, password, url);
			connection = connectionFactory.createConnection();
			// Käivitame yhenduse
			connection.start();

			// 2. Loome sessiooni
			/*
			 * createSession võtab 2 argumenti: 1. kas saame kasutada
			 * transaktsioone 2. automaatne kinnitamine
			 */
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);

			// Loome teadete sihtkoha (järjekorra). Parameetriks järjekorra nimi
			Destination destination = session.createQueue(SUBJECT);
			Destination answerDestination = session.createQueue(SUBJECT_ANSWER);

			// 3. Loome teadete saatja
			MessageProducer producer = session.createProducer(destination);
			MessageConsumer consumer = session.createConsumer(answerDestination);
			consumer.setMessageListener(new MessageListenerImpl()); //paneme uuele consumerile listeneri

			// producer.setDeliveryMode(DeliveryMode.PERSISTENT);
			producer.setTimeToLive(timeToLive);

			// 4. teadete saatmine 
			sendLoop(session, producer);

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}

	
	protected void sendLoop(Session session, MessageProducer producer)
			throws Exception {

		for (int i = 0; i < messageCount || messageCount == 0; i++) {
			ObjectMessage objectMessage = session.createObjectMessage();
			objectMessage.setObject(createTellimus()); // peab olema Serializable
			producer.send(objectMessage);

			TextMessage message = session
					.createTextMessage(createMessageText(i));
			log.debug("Sending message: " + message.getText());
			producer.send(message);
			
			// ootab 1 sekundi
			Thread.sleep(sleepTime);
		}
	}

	private Tellimus createTellimus() {
		Tellimus tellimus = new Tellimus();
		tellimus.addTellimuseRida(new TellimuseRida(new Toode(1, "Joonas Hein ja sõbrad", new BigDecimal(13.37)), new Long(5)));
		tellimus.addTellimuseRida(new TellimuseRida(new Toode(1, "Tarzani päevikud", new BigDecimal(26.26)), new Long(1)));

		return tellimus;
	}

	private String createMessageText(int index) {
		return "Message: " + index + " sent at: " + (new Date()).toString();
	}
	
	/**
	 * Käivitatakse, kui tuleb sõnum
	 */
	class MessageListenerImpl implements javax.jms.MessageListener {

		public void onMessage(Message message) {
			if (message instanceof TextMessage) {
				try {
				TextMessage textMessage = (TextMessage) message;
				String msg = textMessage.getText();
				log.info("Received answer: " + msg);
				} catch (JMSException e) {
					log.warn("Caught: " + e);
					e.printStackTrace();
				}
			}
		}
	}
}
