from __future__ import annotations
import threading as thread
from abc import abstractmethod, ABCMeta, ABC
from queue import Queue
from typing import Any, Callable, Literal as _lit, Optional

from ..exceptions import CannotAddToBuiltBatchException

BatchStatus = _lit["build", "ready", "launch", "done"]

class AbstractReceipt(ABC):

    @abstractmethod
    def get(self):
        pass

class MockReceipt(AbstractReceipt):
    def __init__(self, returned: Optional[Any] = None, return_factory: Optional[Callable[[], Any]] = None) -> None:
        self.return_factory = return_factory or (lambda: returned)

    def get(self):
        return self.return_factory()

class Receipt(AbstractReceipt):
    def __init__(self, batch, args):
        self.batch: AbstractBatch = batch
        self.args = args

    def get(self):
        match self.batch.status:
            case "build" | "ready":
                self.batch.launch_here()
            case "launch":
                self.batch.executor.join()
            case "done":
                pass
        return self.batch.get_value(self.args)

class _QueueForEverySublass(ABCMeta):
    def __init__(cls, name, bases, attrs):
        super().__init__(name, bases, attrs)
        cls._batches = Queue()
        cls.batch_cache_lock = thread.Lock()
        cls.batch_cache = {}

class AbstractBatch(metaclass=_QueueForEverySublass):
    """
    AbstractBatch is an abstract base class that represents a batch processing unit.

    It provides common functionality for managing the status of the batch, adding items to the batch,
    and launching the batch processing asynchronously.

    Subclasses of AbstractBatch should implement the abstract methods `limit`, `fetch`, and `get_value`.
    """
    CACHING = True

    def __init__(self):
        self.status: BatchStatus = "build"
        self.container: list[tuple[Any]] = []
        self.executor: thread.Thread | None = None

    @abstractmethod
    def limit(self):
        """
        The maximum number of items that the batch can hold.

        Returns:
            int: The limit of the batch.
        """
        pass

    @abstractmethod
    def fetch(self):
        """
        Fetches required items.

        This method should be implemented by subclasses to define how items are fetched.
        """
        pass

    def launch_here(self):
        """
        Launches the batch processing.
        """
        self.status = "launch"
        self.fetch()
        self.save_to_cache()
        self.status = "done"

    def save_to_cache(self):
        self._save_to_cache(self)

    @classmethod
    def _save_to_cache(cls, batch: AbstractBatch):
        if cls.CACHING:
            with cls.batch_cache_lock:
                cls.batch_cache |= {tuple(i.values()):batch for i in batch.container}

    def add(self, **kwargs):
        """
        Adds items to the batch.

        Args:
            **kwargs: keyword arguments to be added to the batch.

        Returns:
            Receipt: A receipt object representing the added item.
        """
        if self.status != "build":
            raise CannotAddToBuiltBatchException(self)
        self.container.append(kwargs)
        if len(self.container) > self.limit():
            self.status = "ready"
            self.launch()
        return Receipt(self, kwargs)

    def launch(self):
        """
        Launches the batch processing asynchronously.
        """
        self.executor = thread.Thread(target=self.launch_here)
        self.executor.start()

    @abstractmethod
    def get_value(self, kwargs):
        """
        Retrieves the value associated with the given items.
        This method should be implemented based on how fetch works.
        """
        pass

    @classmethod
    def request(cls, **kwargs) -> Receipt:
        """
        Creates a receipt and adds item to a batch.

        If there is no existing batch or the existing batch is not in "build" status,
        a new batch is created. Otherwise, the existing batch is used.

        Args:
            args: The items to be added to the batch.

        Returns:
            Receipt: A receipt object representing the added items.
        """
        cache_key = tuple(kwargs.values())
        if cache_key in cls.batch_cache:
            return Receipt(cls.batch_cache[cache_key], kwargs)
        if cls._batches.empty() or cls._batches.queue[-1].status != "build":
            cls._batches.put(cls())
        return cls._batches.queue[-1].add(**kwargs)
