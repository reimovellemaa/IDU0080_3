package ee.ttu.idu0080.raamatupood.client;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.ObjectMessage;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.apache.activemq.ActiveMQConnection;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.log4j.Logger;

import ee.ttu.idu0080.raamatupood.server.EmbeddedBroker;
import ee.ttu.idu0080.raamatupood.types.Car;

/**
 * JMS s�numite tarbija. �hendub broker-i urlile
 * 
 * @author Allar Tammik
 * @date 08.03.2010
 */
public class Consumer {
	private static final Logger log = Logger.getLogger(Consumer.class);
	private String SUBJECT = "UUSJRK";
	private String user = ActiveMQConnection.DEFAULT_USER;
	private String password = ActiveMQConnection.DEFAULT_PASSWORD;
	private String url = EmbeddedBroker.URL;

	public static void main(String[] args) {
		Consumer consumerTool = new Consumer();
		consumerTool.run();
	}

	public void run() {
		Connection connection = null;
		try {
			log.info("Connecting to URL: " + url);
			log.info("Consuming queue : " + SUBJECT);

			// 1. Loome �henduse
			ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
					user, password, url);
			connection = connectionFactory.createConnection();

			// Kui �hendus kaob, l�petatakse Consumeri t�� veateatega.
			connection.setExceptionListener(new ExceptionListenerImpl());

			// K�ivitame �henduse
			connection.start();

			// 2. Loome sessiooni
			/*
			 * createSession v�tab 2 argumenti: 1. kas saame kasutada
			 * transaktsioone 2. automaatne kinnitamine
			 */
			Session session = connection.createSession(false,
					Session.AUTO_ACKNOWLEDGE);

			// Loome teadete sihtkoha (j�rjekorra). Parameetriks j�rjekorra nimi
			Destination destination = session.createQueue(SUBJECT);

			// 3. Teadete vastuv�tja
			MessageConsumer consumer = session.createConsumer(destination);

			// Kui teade vastu v�etakse k�ivitatakse onMessage()
			consumer.setMessageListener(new MessageListenerImpl());

		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
	
	

	/**
	 * K�ivitatakse, kui tuleb s�num
	 */
	class MessageListenerImpl implements javax.jms.MessageListener {

		public void onMessage(Message message) {
			try {
				if (message instanceof TextMessage) {
					TextMessage txtMsg = (TextMessage) message;
					String msg = txtMsg.getText();
					log.info("Received: " + msg);
				} else if (message instanceof ObjectMessage) {
					ObjectMessage objectMessage = (ObjectMessage) message;
					if(objectMessage.getObject() instanceof Car)
					{
					    Car auto=(Car)objectMessage.getObject();
						log.info("Auto!!! Autol on "+auto.getDoors()+" uks/ust");
					}
					
						
					String msg = objectMessage.getObject().toString();
					log.info("Received: " + msg);

				} else {
					log.info("Received: " + message);
				}

			} catch (JMSException e) {
				log.warn("Caught: " + e);
				e.printStackTrace();
			}
		}
	}

	/**
	 * K�ivitatakse, kui tuleb viga.
	 */
	class ExceptionListenerImpl implements javax.jms.ExceptionListener {

		public synchronized void onException(JMSException ex) {
			log.error("JMS Exception occured. Shutting down client.");
			ex.printStackTrace();
		}
	}

}