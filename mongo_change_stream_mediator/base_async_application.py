from abc import abstractmethod, ABC


class BaseAsyncApplication(ABC):
    def __init__(self):
        self._should_run = False

    async def start(self):
        await self._start_dependencies()
        self._should_run = True

    async def stop(self):
        self._should_run = False
        await self._stop_dependencies()

    @abstractmethod
    def exit_gracefully(self, signum, frame):
        raise NotImplementedError

    @abstractmethod
    async def _start_dependencies(self):
        raise NotImplementedError

    @abstractmethod
    async def _stop_dependencies(self):
        raise NotImplementedError

    @abstractmethod
    async def task(self):
        raise NotImplementedError
