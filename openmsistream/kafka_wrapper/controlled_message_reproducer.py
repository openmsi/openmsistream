"""
A ConsumerAndProducerGroup whose receipt and production of messages
is managed using the ControlledProcessInfrastructure
"""

#imports
import time
from abc import ABC, abstractmethod
from queue import Queue
from ..workflow.config import RUN_CONST
from ..workflow.controlled_process_multi_threaded import ControlledProcessMultiThreaded
from .consumer_and_producer_group import ConsumerAndProducerGroup

class ControlledMessageReproducer(ControlledProcessMultiThreaded,ConsumerAndProducerGroup,ABC) :
    """
    An abstract base class combining and ControlledProcessMultiThreaded and a ConsumerAndProducerGroup
    to systematically read messages and produce others.
    """

    CONSUMER_POLL_TIMEOUT = 0.050
    NO_MESSAGE_WAIT = 0.005
    FLUSH_PRODUCER_EVERY = 100 #flush the producer after this many calls to produce_from_queue (could be fast)
    PRODUCER_FLUSH_TIMEOUT = 0.050 #timeout for the intermediate calls to producer.flush

    def __init__(self,config_path,consumer_topic_name,producer_topic_name,*,
                 n_producer_threads=1,n_consumer_threads=RUN_CONST.DEFAULT_N_THREADS,**kwargs) :
        """
        Hang onto the number of messages read, processed, and produced
        """
        self.n_msgs_read = 0
        self.n_msgs_processed = 0
        self.n_msgs_produced = 0
        super().__init__(config_path,consumer_topic_name,n_threads=max(n_producer_threads,n_consumer_threads),**kwargs)
        self.restart_at_beginning = False #set to true to reset new consumers to their earliest offsets
        self.message_key_regex = None #set to some regex to filter messages by their keys
        self.filter_new_messages = False #reset the regex after the consumer has filtered through previous messages
        self.last_message = None #hold onto the last consumed message to commit its offset on shutdown
        self.producer_topic_name = producer_topic_name
        self.n_producer_threads = n_producer_threads
        self.n_consumer_threads = n_consumer_threads
        self.producer_message_queue = Queue()

    def _run_worker(self,create_consumer=False,create_producer=False,
                    produce_from_queue_args=None,produce_from_queue_kwargs=None):
        """
        Handle optional startup and shutdown of thread-independent Consumer and/or Producer.
        Serve individual messages to the _process_message function on the Consumer side,
        and produce messages from the shared Queue on the Producer side.
        """
        #create the Consumer and/or Producer for this thread
        if self.alive :
            consumer, producer = self.__get_thread_consumer_and_producer(create_consumer,create_producer)
        #start the loop for while the controlled process is alive
        calls_since_producer_flush = 0
        while self.alive :
            #if this thread has a Consumer side
            if consumer is not None :
                self.__consume_messages_while_alive(consumer)
            #if this thread has a Producer side
            if producer is not None :
                calls_since_producer_flush = self.__produce_messages_while_alive(producer,
                                                                                 produce_from_queue_args,
                                                                                 produce_from_queue_kwargs,
                                                                                 calls_since_producer_flush)
        #commit the offset of the last message received if it wasn't already consumed in the past (block until done)
        if ( (consumer is not None) and
             (self.last_message is not None) and (not consumer.message_consumed_before(self.last_message)) ) :
            self.__commit_last_message_offset(consumer)
        #shut down the Consumer/Producer that was created once the process isn't alive anymore
        if consumer is not None :
            consumer.close()
        if producer is not None :
            producer.flush(timeout=-1)
            producer.close()

    def _on_shutdown(self):
        super()._on_shutdown()
        self.close()

    @abstractmethod
    def _process_message(self,lock,msg,*args,**kwargs) :
        """
        Process a single message read from the thread-independent Consumer
        Returns true if processing was successful, and False otherwise

        lock = lock across all created child threads (use to enforce thread safety during processing)
        msg  = a single message that was consumed and should be processed by this function

        Not implemented in the base class
        """
        raise NotImplementedError

    def __get_thread_consumer_and_producer(self,create_consumer,create_producer) :
        """
        Return the consumer and producer to use for a specific thread
        """
        if create_consumer :
            consumer = self.get_new_subscribed_consumer(restart_at_beginning=self.restart_at_beginning,
                                                        message_key_regex=self.message_key_regex,
                                                        filter_new_messages=self.filter_new_messages)
        else :
            consumer = None
        if create_producer :
            producer = self.get_new_producer()
        else :
            producer = None
        if ( (consumer is not None) and
             ( ('enable.auto.commit' not in consumer.configs.keys()) or
               (consumer.configs['enable.auto.commit'] is True))) :
            warnmsg = 'WARNING: enable.auto.commit has not been set to False for a Consumer that will manually commit '
            warnmsg+= 'offsets. Missed or duplicate messages could result. You can set "enable.auto.commit"=False in '
            warnmsg+= 'the "consumer" section of the config file to re-enable manual offset commits (recommended).'
            self.logger.warning(warnmsg)
        return consumer, producer

    def __consume_messages_while_alive(self,consumer) :
        """
        Runs continuously while the calling thread is alive to consume messages
        """
        #consume a message from the topic
        msg = consumer.get_next_message(self.CONSUMER_POLL_TIMEOUT)
        if msg is None :
            time.sleep(self.NO_MESSAGE_WAIT) #wait just a bit to not over-tax things
            return
        with self.lock :
            self.n_msgs_read+=1
            self.last_message = msg
        #send the message to the _process_message function
        retval = self._process_message(self.lock,msg)
        #count and (asynchronously) commit the message as processed (if not consumed already in the past)
        if retval :
            with self.lock :
                self.n_msgs_processed+=1
            if not consumer.message_consumed_before(msg) :
                tps = consumer.commit(msg)
                if tps is None :
                    return
                for t_p in tps :
                    if t_p.error is not None :
                        warnmsg = 'WARNING: failed to synchronously commit offset of last message '
                        warnmsg+= f'received on "{t_p.topic}" partition {t_p.partition}. Duplicate '
                        warnmsg+=  'messages may result when this Consumer is restarted. '
                        warnmsg+= f'Error reason: {t_p.error.str()}'
                        self.logger.warning(warnmsg)

    def __produce_messages_while_alive(self,producer,pfq_args,pfq_kwargs,calls_since_flush) :
        """
        Runs continuously while the calling thread is alive to produce messages
        """
        if self.producer_message_queue.empty() :
            time.sleep(self.NO_MESSAGE_WAIT) #wait just a bit to not overtax things
        else :
            pfq_args = [] if pfq_args is None else pfq_args
            pfq_kwargs = {} if pfq_kwargs is None else pfq_kwargs
            producer.produce_from_queue(self.producer_message_queue,self.producer_topic_name,
                                        *pfq_args,**pfq_kwargs)
        if calls_since_flush>=self.FLUSH_PRODUCER_EVERY :
            producer.flush(timeout=self.PRODUCER_FLUSH_TIMEOUT)
            calls_since_flush=0
        else :
            calls_since_flush+=1
        return calls_since_flush

    def __commit_last_message_offset(self,consumer) :
        """
        Commit the offset of the last message received while blocking
        """
        try :
            tps = consumer.commit(self.last_message,asynchronous=False)
            if tps is None :
                return
            for t_p in tps :
                if t_p.error is not None :
                    warnmsg = 'WARNING: failed to synchronously commit offset of last message received on '
                    warnmsg+= f'"{t_p.topic}" partition {t_p.partition}. Duplicate messages may result '
                    warnmsg+= f'when this Consumer is restarted. Error reason: {t_p.error.str()}'
                    self.logger.warning(warnmsg)
        except Exception as exc :
            errmsg = 'WARNING: failed to synchronously commit offset of last message received. '
            errmsg+= 'Duplicate messages may be read the next time this Consumer is started. '
            errmsg+= 'Error will be logged below but not re-raised.'
            self.logger.error(errmsg,exc_obj=exc,reraise=False)
