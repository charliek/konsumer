package smartthings.konsumer.circuitbreaker

import smartthings.konsumer.event.KonsumerEvent
import smartthings.konsumer.event.KonsumerEventListener
import smartthings.konsumer.util.SameThreadExecutionService
import spock.lang.Specification


class SimpleCircuitBreakerSpec extends Specification {

	def 'should set the circuit breaker to the open state after calling open'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()

		when:
		circuitBreaker.open(this.getClass().toString())

		then:
		circuitBreaker.isOpen()
	}

	def 'should close the circuit breaker if no other peers has also opened it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		circuitBreaker.open(this.getClass().toString())

		when:
		circuitBreaker.conditionalClose(this.getClass().toString())

		then:
		!circuitBreaker.isOpen()
	}

	def 'should not be able to close the circuit breaker if another peer has opened it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		circuitBreaker.open('trigger')
		circuitBreaker.open('anotherTrigger')

		when:
		circuitBreaker.conditionalClose('trigger')

		then:
		circuitBreaker.isOpen()
	}

	def 'should close circuit after all sources have tried to close it'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker()
		circuitBreaker.open('trigger')
		circuitBreaker.open('anotherTrigger')

		when:
		circuitBreaker.conditionalClose('trigger')
		circuitBreaker.conditionalClose('anotherTrigger')

		then:
		!circuitBreaker.isOpen()
	}

	def 'should notify listeners when the circuit breaker is opened'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker(new SameThreadExecutionService())
		KonsumerEventListener eventListener = Mock()
		circuitBreaker.addListener(eventListener)

		when:
		circuitBreaker.open(this.getClass().toString())

		then:
		1 * eventListener.eventNotification(KonsumerEvent.STOPPED)
		0 * _
	}

	def 'should notify listeners when the circuit breaker is closed'() {
		given:
		SimpleCircuitBreaker circuitBreaker = new SimpleCircuitBreaker(new SameThreadExecutionService())
		circuitBreaker.open(this.getClass().toString())
		KonsumerEventListener eventListener = Mock()
		circuitBreaker.addListener(eventListener)

		when:
		circuitBreaker.conditionalClose(this.getClass().toString())

		then:
		1 * eventListener.eventNotification(KonsumerEvent.RESUMED)
		0 * _
	}

}
