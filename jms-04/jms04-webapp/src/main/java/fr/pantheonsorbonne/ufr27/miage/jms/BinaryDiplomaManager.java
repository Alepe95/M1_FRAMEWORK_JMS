package fr.pantheonsorbonne.ufr27.miage.jms;

import java.io.Closeable;
import java.io.IOException;

import javax.annotation.PostConstruct;
import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;
import javax.inject.Named;
import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.ConnectionFactory;
import javax.jms.JMSException;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Queue;
import javax.jms.Session;

import com.sun.mail.iap.ByteArray;

import fr.pantheonsorbonne.ufr27.miage.DiplomaInfo;
import fr.pantheonsorbonne.ufr27.miage.dto.BinaryDiplomaDTO;

@ApplicationScoped
public class BinaryDiplomaManager implements Closeable {

	@Inject
	@Named("diplomaRequests")
	private Queue requestsQueue;

	@Inject
	@Named("diplomaFiles")
	private Queue filesQueue;

	@Inject
	private ConnectionFactory connectionFactory;

	private Connection connection;
	private MessageConsumer binDiplomaConsumer;
	private MessageProducer diplomaRequestProducer;

	private Session session;

	@PostConstruct
	void init() {
		try {
			connection = connectionFactory.createConnection("alexis", "perez");
			connection.start();
			session = connection.createSession();
			binDiplomaConsumer = session.createConsumer(filesQueue);
			diplomaRequestProducer = session.createProducer(requestsQueue);
		} catch (JMSException e) {
			throw new RuntimeException(e);
		}

	}

	public BinaryDiplomaDTO consume() {
		try {
			BytesMessage message = (BytesMessage) binDiplomaConsumer.receive();
			byte[] payload = new byte[(int) message.getBodyLength()];
			message.readBytes(payload);

			BinaryDiplomaDTO dto = new BinaryDiplomaDTO();
			dto.setId(message.getIntProperty("id"));
			dto.setData(payload);
			return dto;
		} catch (JMSException e) {
			System.out.println("failed to consume message ");
			return null;
		}
		// receive a Byte Message from the consumer
		// create a byte array sized after the message's payload body length
		// read the message on the byte array
		// create a BinaryDiplomaDTO containing the id of the diploma and the data
		// return the DTO

	
	}

	public void requestBinDiploma(DiplomaInfo info) {

		// create a String writer
		// create a JaxBContext, and bount DiplomaInfo.class
		// create a Marshaller and marshall the class in the writer
		// send a text Message containing the JAXB-marshalled object through the wire

	}

	@Override
	public void close() throws IOException {
		try {
			binDiplomaConsumer.close();
			session.close();
			connection.close();
		} catch (JMSException e) {
			System.out.println("Failed to close JMS resources");
		}

	}

}
