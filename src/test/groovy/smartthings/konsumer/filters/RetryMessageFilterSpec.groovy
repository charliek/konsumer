package smartthings.konsumer.filters

import kafka.message.MessageAndMetadata
import smartthings.konsumer.filterchain.MessageContext
import spock.lang.Specification


class RetryMessageFilterSpec extends Specification {

	MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()
	MessageContext messageContext = Mock()

	def 'message retrying works as expected for different with try count #tryCount'() {
		given:
		RetryMessageFilter messageFilter = new RetryMessageFilter(tryCount)

		when:
		messageFilter.handleMessage(messageAndMetadata, messageContext)

		then:
		thrown(Exception)
		tryCount * messageContext.next(messageAndMetadata) >> { throw new Exception("Error processing message") }
		_ * messageAndMetadata.partition() >> 0
		_ * messageAndMetadata.offset() >> 42
		0 * _

		where:
		tryCount << [1, 2, 3, 4, 5, 6]
	}

	def 'should not retry if processing was successful'() {
		given:
		RetryMessageFilter messageFilter = new RetryMessageFilter(42)

		when:
		messageFilter.handleMessage(messageAndMetadata, messageContext)

		then:
		1 * messageContext.next(messageAndMetadata)
		0 * _
	}

	def 'should reject any tryCount less than 1'() {
		when:
		new RetryMessageFilter(0)

		then:
		def e = thrown(AssertionError)
		0 * _
	}

	def 'should not throw exception after retries if swallowErrors is true'() {
		given:
		RetryMessageFilter messageFilter = new RetryMessageFilter(2, true)

		when:
		messageFilter.handleMessage(messageAndMetadata, messageContext)

		then:
		2 * messageContext.next(messageAndMetadata) >> { throw new Exception("Error processing message") }
		_ * messageAndMetadata.partition() >> 0
		_ * messageAndMetadata.offset() >> 42
		0 * _

	}

}
