import threading
import queue
import time
import logging

from common import middleware


class Sender:
    """Background publisher thread that centralizes AMQP publishes.
    This ensures all basic_publish calls are executed from a single thread/connection.
    """

    def __init__(self, host, exchange_prefix, routing_count, retries=3, backoff=0.5, timeout=10):
        self.host = host
        self.exchange_prefix = exchange_prefix
        self.routing_count = routing_count
        self.retries = retries
        self.backoff = backoff
        self.timeout = timeout

        self._queue = queue.Queue()
        self._thread = threading.Thread(target=self._loop)
        self._stop = threading.Event()
        self._accepting = True

        # publishers cache per routing idx (lazy created in publisher thread)
        self._publishers = [None] * routing_count

    def start(self):
        if not self._thread.is_alive():
            self._thread.start()

    def stop(self, wait=True, timeout=None):
        self._accepting = False
        try:
            self._queue.put(None)
        except Exception:
            pass

        if wait:
            self._thread.join(timeout)

            if self._thread.is_alive():
                logging.warning("Sender did not exit in time. Forcing publisher close")

                # close publishers
                for pub in self._publishers:
                    if pub is not None:
                        try:
                            pub.close()
                        except Exception:
                            pass

    def send(self, idx, payloads, timeout=None):
        """Enqueue a publish task and wait for result synchronously.

        idx: routing index (0..routing_count-1)
        payloads: list of bytes payloads to be published in order
        timeout: seconds to wait for publish to complete
        """
        if not self._accepting:
            raise RuntimeError("Sender is shutting down")

        if idx < 0 or idx >= self.routing_count:
            raise ValueError("invalid idx")

        event = threading.Event()
        task = {"idx": idx, "payloads": payloads, "event": event, "ok": None, "exc": None}
        self._queue.put(task)
        waited = event.wait(timeout or self.timeout)
        if not waited:
            raise TimeoutError("publish timeout")
        if not task["ok"]:
            raise task["exc"] if task["exc"] is not None else Exception("publish failed")
        return True

    def _ensure_publisher(self, idx):
        if self._publishers[idx] is None:
            # create a publisher bound to a single routing key for simplicity
            self._publishers[idx] = middleware.MessageMiddlewareExchangeRabbitMQ(
                self.host, self.exchange_prefix, [f"{self.exchange_prefix}_{idx}"]
            )
        return self._publishers[idx]

    def _loop(self):
        while True:
            task = self._queue.get() # bloqueo hasta tarea (o sentinel)
            
            # sentinel -> señal de apagado ordenado
            if task is None:
                # marcar la tarea como hecha y salir
                try:
                    self._queue.task_done()
                except Exception:
                    pass
                break

            idx = task["idx"]
            payloads = task["payloads"]
            event = task["event"]

            ok = False
            exc = None

            # try with retries, recreate publisher on failure
            try:
                for attempt in range(self.retries):
                    try:
                        pub = self._ensure_publisher(idx)
                        for payload in payloads:
                            pub.send(payload)
                        ok = True
                        break
                    except Exception as e:
                        logging.exception("Sender publish attempt %s failed", attempt)
                        exc = e
                        # try to close and recreate
                        try:
                            if self._publishers[idx] is not None:
                                self._publishers[idx].close()
                        except Exception:
                            pass
                        self._publishers[idx] = None
                        time.sleep(self.backoff * (attempt + 1))
                        continue
            finally:
                # asegurar que el llamador no quede bloqueado y que la cola se mantenga consistente
                task["ok"] = ok
                task["exc"] = exc
                try:
                    event.set()
                except Exception:
                    pass
                try:
                    self._queue.task_done()
                except Exception:
                    pass

        # cleanup before exiting
        for pub in self._publishers:
            if pub is not None:
                try:
                    pub.close()
                except Exception:
                    pass
