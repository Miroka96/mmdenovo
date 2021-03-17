import gc
import multiprocessing
import signal
from multiprocessing.pool import Pool
from typing import Callable, Any, Optional, Tuple, Iterable, List, NoReturn, Union

from mmproteo.utils import log
from mmproteo.utils.config import Config
from mmproteo.utils.log import Logger


class _IndexedItemProcessor:
    def __init__(self, item_processor: Callable[[Any], Union[Optional[str], NoReturn]]):
        self.item_processor = item_processor

    def __call__(self, indexed_item: Tuple[int, Optional[Any]]) -> Optional[Any]:
        index, item = indexed_item
        if item is None:
            response = None
        else:
            try:
                response = self.item_processor(item)
            except KeyboardInterrupt:
                raise
            except Exception as e:
                response = e
        gc.collect()
        return index, response


class ItemProcessor:
    items: Iterable[Optional[Any]]
    indexed_item_processor: _IndexedItemProcessor
    action_name: str
    subject_name: str
    keep_null_values: bool
    count_failed_files: bool
    max_num_items: Optional[int] = None
    logger: Logger
    items_to_process_count: Optional[int] = None
    processed_items: List[Optional[Any]] = list()
    process_pool: Optional[Pool] = None
    action_name_past_form: str

    def __init__(self,
                 items: Iterable[Optional[Any]],
                 item_processor: Callable[[Any], Union[Optional[str], NoReturn]],
                 action_name: str,
                 subject_name: str = "files",
                 max_num_items: Optional[int] = None,
                 keep_null_values: bool = Config.default_keep_null_values,
                 count_failed_files: bool = Config.default_count_failed_files,
                 thread_count: int = Config.default_thread_count,
                 logger: log.Logger = log.DEFAULT_LOGGER):
        if thread_count == 0:
            thread_count = multiprocessing.cpu_count()

        if max_num_items == 0:
            max_num_items = None

        self.items = items
        self.indexed_item_processor = _IndexedItemProcessor(item_processor)
        self.action_name = action_name
        self.subject_name = subject_name
        self.keep_null_values = keep_null_values
        self.count_failed_files = count_failed_files
        self.max_num_items = max_num_items
        self.logger = logger

        if thread_count != 1:
            original_sigint_handler = signal.signal(signal.SIGINT, signal.SIG_IGN)
            self.process_pool = multiprocessing.Pool(processes=thread_count)
            signal.signal(signal.SIGINT, original_sigint_handler)

        if self.action_name.endswith("e"):
            self.action_name_past_form = self.action_name + "d"
        else:
            self.action_name_past_form = self.action_name + "ed"

    def __drop_null_items(self):
        non_null_items = [item for item in self.items if item is not None]
        self.items_to_process_count = len(non_null_items)

        if not self.keep_null_values:
            self.items = non_null_items

        if self.items_to_process_count == 0:
            self.logger.warning(f"No {self.subject_name} available to {self.action_name}")

    def __limit_number_of_items_to_process(self):
        if self.max_num_items is not None:
            self.items_to_process_count = min(self.items_to_process_count, self.max_num_items)
            if self.items_to_process_count < self.max_num_items:
                # limit doesn't get triggered
                self.max_num_items = None

    def __process_item_batch_in_parallel(self, item_batch: Iterable[Tuple[int, Optional[Any]]]) -> List[Optional[Any]]:
        try:
            indexed_results: Iterable[Tuple[int, Optional[Any]]] = self.process_pool.imap_unordered(
                self.indexed_item_processor,
                item_batch)
            results: List[Optional[Any]] = [indexed_item[1] for indexed_item in sorted(indexed_results)]
        except KeyboardInterrupt:
            self.logger.info("Terminating workers")
            self.process_pool.terminate()
            self.process_pool.join()
            raise
        else:
            self.process_pool.close()
            self.process_pool.join()
            return results

    def __process_item_batch(self, item_batch: Iterable[Tuple[int, Optional[Any]]]):
        if self.process_pool is None:
            results = [self.indexed_item_processor(indexed_item) for indexed_item in enumerate(item_batch)]
        else:
            results = self.__process_item_batch_in_parallel(item_batch)
        self.processed_items += results

    def __get_successfully_processed_items(self) -> List[Any]:
        items = [item for item in self.processed_items if item is not None]
        if not self.count_failed_files:
            items = [item for item in items if not isinstance(item, Exception)]
        return items

    def __count_successfully_processed_items(self) -> int:
        return len(self.__get_successfully_processed_items())

    def __process_items(self) -> None:
        indexed_items = enumerate(self.items)
        if self.max_num_items is None:
            self.__process_item_batch(indexed_items)
        else:
            while self.items_to_process_count > 0:
                processed_items_count = len(self.processed_items)
                current_item_batch = indexed_items[processed_items_count:
                                                   processed_items_count + self.items_to_process_count]

                self.__process_item_batch(current_item_batch)

                self.items_to_process_count = self.max_num_items - self.__count_successfully_processed_items()

    def __evaluate_results(self) -> None:
        successfully_processed_items_count = self.__count_successfully_processed_items()
        if successfully_processed_items_count > 0:
            self.logger.info(
                f"Successfully {self.action_name_past_form} {successfully_processed_items_count} {self.subject_name}")
        else:
            self.logger.info(f"No {self.subject_name} were {self.action_name_past_form}")

    def process(self) -> Iterable[Optional[Any]]:
        self.__drop_null_items()
        if self.items_to_process_count == 0:
            return self.items
        self.__limit_number_of_items_to_process()
        self.logger.debug(f"Trying to {self.action_name} {self.items_to_process_count} {self.subject_name}")

        self.__process_items()
        self.__evaluate_results()

        if self.keep_null_values:
            return self.processed_items
        else:
            return self.__get_successfully_processed_items()
