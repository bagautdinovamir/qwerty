import asyncio
import random
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import timedelta
from enum import Enum
from typing import Any
from typing import List

timeout_seconds = timedelta(seconds=15).total_seconds()
THREADS_COUNT = 2


@dataclass
class Payload:
    data: dict | Any


@dataclass
class Address:
    destination: str  # куда


@dataclass
class Event:
    recipients: List[Address]
    payload: Payload


class Result(Enum):
    Accepted = 1
    Rejected = 2

    @classmethod
    def list(cls):
        return [e for e in cls]


def generate_event() -> Event:
    import random
    address = [Address(destination=f'destionation-{i}') for i in range(random.randint(4, 10))]
    payload = Payload(f'payload-{random.randint(10, 20)}')
    event = Event(recipients=address, payload=payload)

    # raise ReadDataException()

    return event


class ReadDataException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


class SendDataException(Exception):
    def __init__(self, msg):
        self.msg = msg
        super().__init__(self.msg)


def read_data() -> Event:
    # Метод для чтения порции данных
    # блокирующая IO-bound задача

    try:
        event = generate_event()
    except ReadDataException:
        # обработка
        ...
    return event


async def send_data_api_client(dest: Address, payload: Payload) -> Result:
    await asyncio.sleep(1)
    # raise SendDataException()
    return random.choice(Result.list())


async def send_data(dest: Address, payload: Payload) -> Result:
    # Метод для рассылки данных
    try:
        res: Result = await send_data_api_client(dest, payload)
    except SendDataException:
        ...

    return res


async def perform_operation() -> None:
    while True:
        with ThreadPoolExecutor(THREADS_COUNT) as pool_executor:
            # loop = asyncio.get_event_loop()
            futures = [pool_executor.submit(read_data) for i in range(2)]
            future_results: list[Event] = []

            for future in as_completed(futures):
                future_results.append(future.result())

        for fut_result in future_results:
            coros_send_data = [send_data(dest=recipient, payload=fut_result.payload) for recipient in
                               fut_result.recipients]

            coros_send_data_result = await asyncio.gather(*coros_send_data, return_exceptions=True)

            retry_send_data_coros = []

            for i in range(len(fut_result.recipients)):
                if coros_send_data_result[i] == Result.Rejected:
                    retry_send_data_coros.append(send_data(dest=fut_result.recipients[i], payload=fut_result.payload))

            if not retry_send_data_coros:
                continue
            else:
                await asyncio.sleep(4)
                await asyncio.gather(*retry_send_data_coros, return_exceptions=True)


if __name__ == '__main__':
    asyncio.run(perform_operation())
