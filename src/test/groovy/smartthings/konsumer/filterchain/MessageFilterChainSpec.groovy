package smartthings.konsumer.filterchain

import kafka.message.MessageAndMetadata
import smartthings.konsumer.MessageProcessor
import smartthings.konsumer.circuitbreaker.CircuitBreaker
import spock.lang.Specification


class MessageFilterChainSpec extends Specification {

	CircuitBreaker circuitBreaker = Mock()
	MessageProcessor messageProcessor = Mock()
	MessageAndMetadata<byte[], byte[]> messageAndMetadata = Mock()

	def 'should call message processor directly if there are no filters'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor)

		when:
		filterChain.handle(messageAndMetadata)

		then:
		1 * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should call every filter in the chain in order before calling the message processor'() {
		given:
		int counter = 0;
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor,
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				assert counter == 0;
				counter++
				ctx.next(messageAndMetadata)
			}
		},
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				assert counter == 1
				counter++
				ctx.next(messageAndMetadata)
			}
		})

		when:
		filterChain.handle(messageAndMetadata)

		then:
		counter == 2
		1 * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should not call message processor if filter does not invoke the next filter in the chain'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor,
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				throw new RuntimeException()
			}
		})

		when:
		filterChain.handle(messageAndMetadata)

		then:
		def e = thrown(RuntimeException)
		0 * _
	}

	def 'should allow filter to retry calls to filters and message processor'() {
		given:
		int numTries = 3
		int filterCounter = 0;
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor,
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				for (int i = 0; i < numTries; i++) {
					ctx.next(messageAndMetadata)
				}
			}
		},
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				filterCounter++
				ctx.next(messageAndMetadata)
			}
		})

		when:
		filterChain.handle(messageAndMetadata)

		then:
		filterCounter == numTries
		numTries * messageProcessor.processMessage(messageAndMetadata)
		0 * _
	}

	def 'should allow filters to open circuit breaker'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor,
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				ctx.circuitBreaker.open('trigger')
			}
		})

		when:
		filterChain.handle(messageAndMetadata)

		then:
		1 * circuitBreaker.open(_)
		0 * _
	}

	def 'should allow filters to close circuit breaker'() {
		given:
		MessageFilterChain filterChain = new MessageFilterChain(circuitBreaker, messageProcessor,
		new MessageFilter() {
			@Override
			void handleMessage(MessageAndMetadata<byte[], byte[]> messageAndMetadata, MessageContext ctx) throws Exception {
				ctx.circuitBreaker.conditionalClose('trigger')
			}
		})

		when:
		filterChain.handle(messageAndMetadata)

		then:
		1 * circuitBreaker.conditionalClose(_)
		0 * _
	}

}
