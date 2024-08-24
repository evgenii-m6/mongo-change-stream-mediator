from abc import abstractmethod, ABC


class BaseApplication(ABC):
    def __init__(self):
        self._should_run = False

    def start(self):
        self._start_dependencies()
        self._should_run = True

    def stop(self):
        self._should_run = False
        self._stop_dependencies()

    @abstractmethod
    def exit_gracefully(self, signum, frame):
        raise NotImplementedError

    @abstractmethod
    def _start_dependencies(self):
        raise NotImplementedError

    @abstractmethod
    def _stop_dependencies(self):
        raise NotImplementedError

    @abstractmethod
    def task(self):
        raise NotImplementedError
