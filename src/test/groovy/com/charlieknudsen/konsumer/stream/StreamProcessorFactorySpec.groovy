package com.charlieknudsen.konsumer.stream

import com.charlieknudsen.konsumer.ListenerConfig
import spock.lang.Specification
import spock.lang.Unroll

class StreamProcessorFactorySpec extends Specification {

	@Unroll
	def 'correct stream method is built (#partionThreads / #processingThreads = #expectedClass)'() {
		given:
		ListenerConfig config = ListenerConfig.builder()
				.partitionThreads(partionThreads)
				.processingThreads(processingThreads)
				.build()

		when:
		StreamProcessor processor = new StreamProcessorFactory(config).processor;

		then:
		processor.class == expectedClass

		where:
		partionThreads | processingThreads | expectedClass
		3              | 3                 | SingleStreamProcessor
		3              | 2                 | SingleStreamProcessor
		3              | -1                | SingleStreamProcessor
		3              | 4                 | ThreadedStreamProcessor
		1              | 120               | ThreadedStreamProcessor
	}
}
