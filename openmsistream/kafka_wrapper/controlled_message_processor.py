"""
A ConsumerAndProducerGroup whose receipt of messages is governed using the ControlledProcess
infrastructure
"""

# imports
from abc import ABC, abstractmethod
from confluent_kafka.serialization import StringSerializer

from ..utilities.heartbeat_producibles import MessageProcessorHeartbeatProducible
from ..utilities.messages import get_message_length
from ..utilities.controlled_processes_heartbeats_logs import (
    ControlledProcessMultiThreadedHeartbeatsLogs,
)
from .consumer_and_producer_group import ConsumerAndProducerGroup


class ControlledMessageProcessor(
    ControlledProcessMultiThreadedHeartbeatsLogs, ConsumerAndProducerGroup, ABC
):
    """
    Combine a ControlledProcessMultiThreadedHeartbeatsLogs and a ConsumerAndProducerGroup
    to create a single interface for reading and processing individual messages
    """

    CONSUMER_POLL_TIMEOUT = 5.0

    def __init__(self, config_path, consumer_topic_name, filepath_regex=None, **kwargs):
        """
        Hang onto the number of messages read and processed
        """
        self.n_msgs_read = 0
        self.n_msgs_processed = 0
        super().__init__(config_path, consumer_topic_name=consumer_topic_name, **kwargs)
        # update heartbeat/log producers
        prod = super().get_new_producer(
            key_serializer_override=StringSerializer(),
            value_serializer_override=StringSerializer(),
        )
        self.__heartbeat_log_producer = prod
        self.set_heartbeat_producer(prod, close_it=True)
        self.set_log_producer(prod, close_it=True)
        # set below to true to reset new consumers to their earliest offsets
        self.restart_at_beginning = False
        # set below to some regex to filter messages by their keys
        self.message_key_regex = None
        # reset the key regex after the consumer has filtered through previous messages
        self.filter_new_message_keys = False
        # set below to some regex to filter messages by filepath
        self.filepath_regex = filepath_regex
        # hold onto the last consumed message to manually commit its offset on shutdown
        self.last_message = None
        # variables for heartbeat messages
        self.n_msgs_read_since_last_heartbeat = 0
        self.n_msgs_processed_since_last_heartbeat = 0
        self.n_bytes_read_since_last_heartbeat = 0
        self.n_bytes_processed_since_last_heartbeat = 0

    def get_heartbeat_message(self):
        new_msg = MessageProcessorHeartbeatProducible(
            self._heartbeat_program_id,
            self.n_msgs_read_since_last_heartbeat,
            self.n_msgs_processed_since_last_heartbeat,
            self.n_bytes_read_since_last_heartbeat,
            self.n_bytes_processed_since_last_heartbeat,
        )
        with self.lock:
            self.n_msgs_read_since_last_heartbeat = 0
            self.n_msgs_processed_since_last_heartbeat = 0
            self.n_bytes_read_since_last_heartbeat = 0
            self.n_bytes_processed_since_last_heartbeat = 0
        return new_msg

    # No override of get_log_message needed

    def _run_worker(self):
        """
        Handle startup and shutdown of a thread-independent Consumer and
        serve individual messages to the _process_message function
        """
        # create the Consumer for this thread
        if self.alive:
            consumer = self.get_new_subscribed_consumer(
                restart_at_beginning=self.restart_at_beginning,
                message_key_regex=self.message_key_regex,
                filter_new_message_keys=self.filter_new_message_keys,
                filepath_regex=self.filepath_regex,
            )
        if ("enable.auto.commit" not in consumer.configs.keys()) or (
            consumer.configs["enable.auto.commit"] is True
        ):
            warnmsg = (
                "WARNING: enable.auto.commit has not been set to False for a Consumer "
                "that will manually commit offsets. Missed or duplicate messages could result. "
                'You can set "enable.auto.commit"=False in the "consumer" section of the '
                "config file to re-enable manual offset commits (recommended)."
            )
            self.logger.warning(warnmsg)
        # start the loop for while the controlled process is alive
        while self.alive:
            self.__do_alive_loop_iteration(consumer)
        # commit the offset of the last message received
        if (self.last_message is not None) and (
            not consumer.message_consumed_before(self.last_message)
        ):
            self.__commit_last_message_offset(consumer)
        # shut down the Consumer that was created once the process isn't alive anymore
        consumer.close()

    def _on_shutdown(self):
        self.close()
        super()._on_shutdown()

    @abstractmethod
    def _process_message(self, lock, msg, *args, **kwargs):
        """
        Process a single message read from the thread-independent Consumer
        Returns true if processing was successful, and False otherwise

        lock = lock across all created child threads
        msg  = a single message that was consumed and should be processed by this function

        Not implemented in the base class
        """
        raise NotImplementedError

    def __do_alive_loop_iteration(self, consumer):
        """
        Helper function to perform actions that happen continuously while the process is alive
        """
        self.__heartbeat_log_producer.poll(0)
        # consume a message from the topic
        msg = consumer.get_next_message(self.CONSUMER_POLL_TIMEOUT)
        if msg is None:
            return
        with self.lock:
            self.n_msgs_read += 1
            self.n_msgs_read_since_last_heartbeat += 1
            self.n_bytes_read_since_last_heartbeat += get_message_length(msg)
            self.last_message = msg
        # send the message to the _process_message function
        retval = self._process_message(self.lock, msg)
        # count and (asynchronously) commit the message as processed
        if retval:
            with self.lock:
                self.n_msgs_processed += 1
                self.n_msgs_processed_since_last_heartbeat += 1
                self.n_bytes_processed_since_last_heartbeat += get_message_length(msg)
            if not consumer.message_consumed_before(msg):
                tps = consumer.commit(msg)
                if tps is None:
                    return
                for t_p in tps:
                    if t_p.error is not None:
                        warnmsg = (
                            f"WARNING: failed to synchronously commit offset of last message "
                            f'received on "{t_p.topic}" partition {t_p.partition}. '
                            "Duplicate messages may result when this Consumer is restarted. "
                            f"Error reason: {t_p.error.str()}"
                        )
                        self.logger.warning(warnmsg)

    def __commit_last_message_offset(self, consumer):
        """
        Commit the offset of the last message received if it wasn't already consumed in the past
        (block until done)
        """
        try:
            tps = consumer.commit(self.last_message, asynchronous=False)
            if tps is not None:
                for t_p in tps:
                    if t_p.error is not None:
                        warnmsg = (
                            f"WARNING: failed to synchronously commit offset of last message "
                            f'received on "{t_p.topic}" partition {t_p.partition}. '
                            "Duplicate messages may result when this Consumer is restarted. "
                            "Error reason: {t_p.error.str()}"
                        )
                        self.logger.warning(warnmsg)
        except Exception as exc:
            warnmsg = (
                "WARNING: failed to synchronously commit offset of last message received. "
                "Duplicate messages may be read the next time this Consumer is started. "
                "Error will be logged below but not re-raised."
            )
            self.logger.warning(warnmsg, exc_info=exc)

    @classmethod
    def get_command_line_arguments(cls):
        superargs, superkwargs = super().get_command_line_arguments()
        args = [
            *superargs,
            "download_regex",
        ]
        return args, superkwargs

    @classmethod
    def get_init_args_kwargs(cls, parsed_args):
        superargs, superkwargs = super().get_init_args_kwargs(parsed_args)
        kwargs = {
            **superkwargs,
            "filepath_regex": parsed_args.download_regex,
        }
        return superargs, kwargs
