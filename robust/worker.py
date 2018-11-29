import logging
import select
import signal
import threading
import time
from queue import Queue
from typing import Any, List, Union

from django.conf import settings
from django.db import (
    DEFAULT_DB_ALIAS, connection,
    connections, transaction,
)
from django.db.backends.postgresql.base import DatabaseWrapper

from .beat import BeatThread, get_scheduler
from .models import Task

logger = logging.getLogger(__name__)

stop_worker = object()


def detach_connection() -> DatabaseWrapper:
    res = connections[DEFAULT_DB_ALIAS]
    res.allow_thread_sharing = True
    del connections[DEFAULT_DB_ALIAS]
    return res


def attach_connection(conn: DatabaseWrapper) -> None:
    connections[DEFAULT_DB_ALIAS] = conn


class WorkerThread(threading.Thread):
    def __init__(self, task_queue: Queue, number: int,
                 runner_cls: type) -> None:
        super(WorkerThread, self).__init__(
            name='WorkerThread-{}'.format(number))
        self.task_queue = task_queue
        self.runner_cls = runner_cls

    def run(self) -> None:
        worker_failure_timeout = getattr(
            settings, 'ROBUST_WORKER_FAILURE_TIMEOUT', 5)

        while True:
            # noinspection PyBroadException
            try:
                tasks, detached_connection = self.task_queue.get()

                if tasks is stop_worker:
                    self.task_queue.task_done()
                    break

                try:
                    attach_connection(detached_connection)

                    logger.debug('%s got tasks %r', self.name, tasks)

                    for task in tasks:
                        runner = self.runner_cls(task)
                        runner.run()
                    transaction.commit()

                except Exception:
                    transaction.rollback()
                    raise

                finally:
                    self.task_queue.task_done()
                    connections.close_all()

            except Exception:
                logger.error('%s exception ', self.name, exc_info=True)
                time.sleep(worker_failure_timeout)

        connections.close_all()
        logger.debug('terminated %s', self.name)


def terminate_notify() -> None:
    with connection.cursor() as cursor:
        logger.warning('terminate notify')
        cursor.execute('NOTIFY robust')


def run_worker(concurrency: int, bulk: int, limit: int, runner_cls: type,
               beat: bool) -> None:
    notify_timeout = getattr(settings, 'ROBUST_NOTIFY_TIMEOUT', 10)

    task_queue: Queue = Queue(maxsize=concurrency)

    threads: List[Union[BeatThread, WorkerThread]] = []

    listen_connection = detach_connection()

    beat_thread = None
    if beat:
        scheduler = get_scheduler()
        beat_thread = BeatThread(scheduler)
        threads.append(beat_thread)
        beat_thread.start()

    for number in range(concurrency):
        worker_thread = WorkerThread(task_queue, number, runner_cls)
        threads.append(worker_thread)
        worker_thread.start()

    should_terminate = []

    def signal_handler(*_: Any) -> None:
        logger.warning('terminate worker')
        should_terminate.append(True)
        terminate_notify()

    signal.signal(signal.SIGINT, signal_handler)

    while True:
        logger.debug('poll')

        if should_terminate:
            if beat_thread is not None:
                beat_thread.terminate = True
            for i in range(concurrency):
                task_queue.put((stop_worker, None))
            break

        if transaction.get_autocommit():
            transaction.set_autocommit(False)

        tasks = Task.objects.next(limit=bulk)

        if not tasks:
            with listen_connection.cursor() as cursor:
                cursor.execute('LISTEN robust')

            logger.debug('listen for postgres events')
            if select.select([listen_connection.connection], [], [],
                             notify_timeout) == ([], [], []):
                logger.debug('listen for postgres events timeout')
            else:
                listen_connection.connection.poll()
                while listen_connection.connection.notifies:
                    listen_connection.connection.notifies.pop()
                logger.debug('listen for postgres events got notify')

            continue

        detached_connection = detach_connection()
        task_queue.put((tasks, detached_connection))

        if limit is not None:
            limit -= len(tasks)
            if limit <= 0:
                logger.warning('terminate worker due to limit')
                should_terminate.append(True)

    logger.debug('join task queue')
    task_queue.join()

    for thread in threads:
        logger.debug('join worker %r', thread)
        thread.join()

    connection.close()
    listen_connection.close()
