package smartthings.konsumer.stream

import kafka.consumer.ConsumerIterator
import kafka.consumer.KafkaStream
import kafka.message.MessageAndMetadata
import smartthings.konsumer.ListenerConfig
import smartthings.konsumer.MessageProcessor
import spock.lang.Specification
import spock.lang.Unroll

import java.util.concurrent.Executor

class ThreadedMessageConsumerSpec extends Specification {

	KafkaStream<byte[], byte[]> stream = Mock()
	ConsumerIterator<byte[], byte[]> streamIterator = Mock()
	MessageProcessor processor = Mock()
	MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()
	Executor currentTreadExecutor = new Executor() {
		@Override
		void execute(Runnable command) {
			command.run()
		}
	}

	@Unroll
	def 'message retrying works as expected for different with try count #tryCount'() {
		given:
		int cnt = 0
		ListenerConfig config = ListenerConfig.builder().tryCount(tryCount).processingThreads(1).build()
		ThreadedMessageConsumer consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, config, processor)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >> { cnt += 1; return cnt == 1 }
		1 * streamIterator.next() >> messageAndMetadata
		tryCount * processor.processMessage(messageAndMetadata) >> { throw new Exception("Error processing message") }
		0 * _

		cnt == 2

		where:
		tryCount << [1, 2, 3, 4, 5, 6]
	}

	def 'successful processing will not retry'() {
		given:
		int cnt = 0
		ListenerConfig config = ListenerConfig.builder().tryCount(tryCount).processingThreads(1).build()
		ThreadedMessageConsumer consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, config, processor)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >> { cnt += 1; return cnt == 1 }
		1 * streamIterator.next() >> messageAndMetadata
		1 * processor.processMessage(messageAndMetadata)
		0 * _

		cnt == 2

		where:
		tryCount << [1, 2, 3, 4, 5, 6]
	}

	def 'try count less than one will be processed once'() {
		given:
		int cnt = 0
		int tryCount = -1
		ListenerConfig config = ListenerConfig.builder().tryCount(tryCount).processingThreads(1).build()
		ThreadedMessageConsumer consumer = new ThreadedMessageConsumer(stream, currentTreadExecutor, config, processor)

		when:
		consumer.run()

		then:
		1 * stream.iterator() >> streamIterator
		2 * streamIterator.hasNext() >> { cnt += 1; return cnt == 1 }
		1 * streamIterator.next() >> messageAndMetadata
		1 * processor.processMessage(messageAndMetadata) >> { throw new Exception("Error processing message") }
		0 * _

		cnt == 2
	}

}
