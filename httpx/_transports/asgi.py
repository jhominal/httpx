from __future__ import annotations

import types
import typing
from functools import partial

from .._models import Request, Response
from .._types import AsyncByteStream
from .base import AsyncBaseTransport

_T_co = typing.TypeVar("_T_co", covariant=True)
_T = typing.TypeVar("_T")
_U = typing.TypeVar("_U")

if typing.TYPE_CHECKING:  # pragma: no cover
    import asyncio

    import anyio.abc
    import anyio.streams.memory
    import trio

    Event = typing.Union[asyncio.Event, trio.Event]
    MemoryObjectReceiveStream = typing.Union[
        anyio.streams.memory.MemoryObjectReceiveStream[_T],
        trio.MemoryReceiveChannel[_T],
    ]
    MemoryObjectSendStream = typing.Union[
        anyio.streams.memory.MemoryObjectSendStream[_T], trio.MemorySendChannel[_T]
    ]
    TaskGroup = typing.Union[anyio.abc.TaskGroup, trio.Nursery]

_Message = typing.MutableMapping[str, typing.Any]
_Receive = typing.Callable[[], typing.Awaitable[_Message]]
_Send = typing.Callable[
    [typing.MutableMapping[str, typing.Any]], typing.Awaitable[None]
]
_ASGIApp = typing.Callable[
    [typing.MutableMapping[str, typing.Any], _Receive, _Send], typing.Awaitable[None]
]

__all__ = ["ASGITransport"]


def is_running_trio() -> bool:
    try:
        # sniffio is a dependency of trio.

        # See https://github.com/python-trio/trio/issues/2802
        import sniffio

        if sniffio.current_async_library() == "trio":
            return True
    except ImportError:  # pragma: nocover
        pass

    return False


def create_event() -> Event:
    if is_running_trio():
        import trio

        return trio.Event()

    import asyncio

    return asyncio.Event()


def create_memory_object_stream(
    max_buffer_size: float,
) -> typing.Tuple["MemoryObjectSendStream[bytes]", "MemoryObjectReceiveStream[bytes]"]:
    if sniffio.current_async_library() == "trio":
        import trio

        return trio.open_memory_channel[bytes](max_buffer_size)

    else:
        import anyio

        return anyio.create_memory_object_stream[bytes](max_buffer_size)


def create_task_group() -> typing.AsyncContextManager["TaskGroup"]:
    if sniffio.current_async_library() == "trio":
        import trio

        return trio.open_nursery()

    else:
        import anyio

        return anyio.create_task_group()


class _Outcome(typing.Protocol[_T_co]):
    def unwrap(self) -> _T_co:
        ...

    def send(
        self, generator: typing.Generator[typing.Any, _T_co, typing.Any]
    ) -> typing.Any:
        ...


class _Success(_Outcome[_T_co]):
    def __init__(self, value: _T_co) -> None:
        self._value = value

    def unwrap(self) -> _T_co:
        return self._value

    def send(
        self, generator: typing.Generator[typing.Any, _T_co, typing.Any]
    ) -> typing.Any:
        return generator.send(self._value)


class _Error(_Outcome[typing.Any]):
    def __init__(self, exception: BaseException) -> None:
        self._exception = exception

    def unwrap(self) -> typing.Any:
        raise self._exception

    def send(
        self, generator: typing.Generator[typing.Any, typing.Any, typing.Any]
    ) -> typing.Any:
        return generator.throw(self._exception)


async def _race(
    first: typing.Callable[[], typing.Awaitable[_T]],
    second: typing.Callable[[], typing.Awaitable[_U]],
) -> typing.Tuple[typing.Optional[_Outcome[_T]], typing.Optional[_Outcome[_U]]]:
    outcomes: typing.List[typing.Optional[_Outcome[typing.Any]]] = [None, None]

    async with create_task_group() as task_group:

        async def set_outcome_and_cancel(
            outcome_index: int, f: typing.Callable[[], typing.Awaitable[typing.Any]]
        ) -> None:
            try:
                outcomes[outcome_index] = _Success(await f())
            except Exception as ex:
                outcomes[outcome_index] = _Error(ex)
            finally:
                task_group.cancel_scope.cancel()

        task_group.start_soon(set_outcome_and_cancel, 0, first)
        task_group.start_soon(set_outcome_and_cancel, 1, second)

    return outcomes[0], outcomes[1]


@types.coroutine
def _async_yield(
    obj: typing.Any,
) -> typing.Generator[typing.Any, typing.Any, typing.Any]:
    return (yield obj)


async def _wait_for_yield_item(item: typing.Any) -> typing.Any:
    if sniffio.current_async_library() == "trio":
        raise RuntimeError("Not possible to implement wait for yield item in trio")
    else:
        import asyncio

        if item is None:
            return

        if not asyncio.isfuture(item):
            raise TypeError(
                "Expected asyncio.Future-like object for item, got"
                f"{type(item)}: {item!r}"
            )

        event = asyncio.Event()

        def set_event(_future: asyncio.Future[typing.Any]) -> None:
            event.set()

        item.add_done_callback(set_event)

        try:
            await event.wait()
            return item.result()
        finally:
            item.remove_done_callback(set_event)


_UNSET = object()


class _AwaitableRunner(typing.Generic[_T_co]):
    def __init__(self, awaitable: typing.Awaitable[_T_co]):
        self._generator = awaitable.__await__()
        self._send_outcome: typing.Optional[_Outcome[typing.Any]] = _Success(None)
        self._yield_item: typing.Any = _UNSET
        self._final_outcome: typing.Optional[_Outcome[_T_co]] = None

    @property
    def finished(self) -> bool:
        return self._final_outcome is not None

    async def _race_yield_item_with(
            self, condition: typing.Callable[[], typing.Awaitable[_U]]
    ) -> typing.Optional[_Outcome[_U]]:
        if sniffio.current_async_library() == "trio":
            return await self._race_yield_item_with_trio(condition)
        else:
            return await self._race_yield_item_with_asyncio(condition)

    async def _race_yield_item_with_trio(
        self, condition: typing.Callable[[], typing.Awaitable[_U]]
    ) -> typing.Optional[_Outcome[_U]]:
        from trio.lowlevel import current_task, reschedule, wait_task_rescheduled
        from trio._core._run import WaitTaskRescheduled

        if not isinstance(self._yield_item, WaitTaskRescheduled):
            try:
                value = await _async_yield(self._yield_item)
                self._send_outcome = _Success(value)
            except BaseException as ex:
                self._send_outcome = _Error(ex)
            finally:
                self._yield_item = _UNSET
            return None

        # yield item is a task reschedule call - meaning that a setup
        # has already been done for rescheduling this task once the condition
        # is fulfilled.

        async with create_task_group() as task_group:
            condition_outcome: typing.Optional[_Outcome[_U]] = None
            main_task = current_task()
            main_task_rescheduled = False

            async def set_event_on_condition_complete():
                nonlocal condition_outcome, main_task_rescheduled
                try:
                    value = await condition()
                    condition_outcome = _Success(value)
                except BaseException as ex:
                    condition_outcome = _Error(ex)
                finally:
                    if not main_task_rescheduled:
                        main_task_rescheduled = True
                        reschedule(main_task, )

            await condition_complete.wait()

    async def _race_yield_item_with_asyncio(
        self, condition: typing.Callable[[], typing.Awaitable[_U]]
    ) -> typing.Optional[_Outcome[_U]]:
        yield_item_outcome, condition_outcome = await _race(
            partial(_wait_for_yield_item, self._yield_item), condition
        )

        if yield_item_outcome is not None:
            self._send_outcome = yield_item_outcome
            self._yield_item = _UNSET

        return condition_outcome

    def _run_generator_step(self) -> None:
        if self._final_outcome is not None or self._send_outcome is None:
            return

        try:
            self._yield_item = self._send_outcome.send(self._generator)
        except StopIteration as ex:
            self._final_outcome = _Success(ex.value)
        except BaseException as ex:
            self._final_outcome = _Error(ex)
        finally:
            self._send_outcome = None

    @types.coroutine
    def run_to_completion(self) -> typing.Generator[typing.Any, typing.Any, _T_co]:
        while self._final_outcome is None:
            if self._yield_item is not _UNSET:
                try:
                    value = yield self._yield_item
                    self._send_outcome = _Success(value)
                except BaseException as ex:
                    self._send_outcome = _Error(ex)
                finally:
                    self._yield_item = _UNSET

            self._run_generator_step()

        return self._final_outcome.unwrap()

    async def run_until(
        self, condition: typing.Callable[[], typing.Awaitable[_U]]
    ) -> typing.Optional[_Outcome[_U]]:
        """
        Run the awaitable until either it is consumed, or condition has resolved

        In case the condition has resolved, returns the condition's outcome.
        In case the generator was consumed, returns None.
        """

        while self._final_outcome is None:
            if self._yield_item is not _UNSET:
                condition_outcome = await self._race_yield_item_with(condition)

                if condition_outcome is not None:
                    return condition_outcome

            self._run_generator_step()

        return None


class ASGIResponseStream(AsyncByteStream):
    def __init__(
        self,
        body_stream: "MemoryObjectReceiveStream[bytes]",
        raise_app_exceptions: bool,
        response_complete: "Event",
        app_runner: _AwaitableRunner[None],
    ) -> None:
        self._body_stream = body_stream
        self._raise_app_exceptions = raise_app_exceptions
        self._response_complete = response_complete
        self._app_runner = app_runner

    async def _wait_for_next_chunk(self) -> typing.Optional[bytes]:
        ClosedResourceError: typing.Type[Exception]
        EndOfStream: typing.Type[Exception]

        if sniffio.current_async_library() == "trio":
            from trio import ClosedResourceError, EndOfChannel as EndOfStream
        else:
            from anyio import ClosedResourceError, EndOfStream

        try:
            return await self._body_stream.receive()
        except (ClosedResourceError, EndOfStream):
            return None

    async def __aiter__(self) -> typing.AsyncIterator[bytes]:
        while True:
            next_chunk_outcome = await self._app_runner.run_until(
                self._wait_for_next_chunk
            )
            if next_chunk_outcome is None:
                break
            next_chunk = next_chunk_outcome.unwrap()
            if next_chunk is None:
                break
            yield next_chunk

        await self.aclose()

    async def aclose(self) -> None:
        self._response_complete.set()
        try:
            await self._app_runner.run_to_completion()
        except Exception:  # noqa: PIE786
            if self._raise_app_exceptions:
                raise


class ASGITransport(AsyncBaseTransport):
    """
    A custom AsyncTransport that handles sending requests directly to an ASGI app.

    ```python
    transport = httpx.ASGITransport(
        app=app,
        root_path="/submount",
        client=("1.2.3.4", 123)
    )
    client = httpx.AsyncClient(transport=transport)
    ```

    Arguments:

    * `app` - The ASGI application.
    * `raise_app_exceptions` - Boolean indicating if exceptions in the application
       should be raised. Default to `True`. Can be set to `False` for use cases
       such as testing the content of a client 500 response.
    * `root_path` - The root path on which the ASGI application should be mounted.
    * `client` - A two-tuple indicating the client IP and port of incoming requests.
    ```
    """

    def __init__(
        self,
        app: _ASGIApp,
        raise_app_exceptions: bool = True,
        root_path: str = "",
        client: tuple[str, int] = ("127.0.0.1", 123),
    ) -> None:
        self.app = app
        self.raise_app_exceptions = raise_app_exceptions
        self.root_path = root_path
        self.client = client

    async def handle_async_request(
        self,
        request: Request,
    ) -> Response:
        assert isinstance(request.stream, AsyncByteStream)

        # ASGI scope.
        scope = {
            "type": "http",
            "asgi": {"version": "3.0"},
            "http_version": "1.1",
            "method": request.method,
            "headers": [(k.lower(), v) for (k, v) in request.headers.raw],
            "scheme": request.url.scheme,
            "path": request.url.path,
            "raw_path": request.url.raw_path.split(b"?")[0],
            "query_string": request.url.query,
            "server": (request.url.host, request.url.port),
            "client": self.client,
            "root_path": self.root_path,
        }

        # Request.
        request_body_chunks = request.stream.__aiter__()
        request_complete = False

        # Response.
        status_code = None
        response_headers = None
        body_send_stream, body_receive_stream = create_memory_object_stream(0)
        response_started = create_event()
        response_complete = create_event()

        # ASGI callables.

        async def receive() -> dict[str, typing.Any]:
            nonlocal request_complete

            if request_complete:
                await response_complete.wait()
                return {"type": "http.disconnect"}

            try:
                body = await request_body_chunks.__anext__()
            except StopAsyncIteration:
                request_complete = True
                return {"type": "http.request", "body": b"", "more_body": False}
            return {"type": "http.request", "body": body, "more_body": True}

        async def send(message: typing.MutableMapping[str, typing.Any]) -> None:
            nonlocal status_code, response_headers

            if message["type"] == "http.response.start":
                assert not response_started.is_set()

                status_code = message["status"]
                response_headers = message.get("headers", [])
                response_started.set()

            elif (
                message["type"] == "http.response.body"
                and not response_complete.is_set()
            ):
                body = message.get("body", b"")
                more_body = message.get("more_body", False)

                if body and request.method != "HEAD":
                    await body_send_stream.send(body)

                if not more_body:
                    response_complete.set()
                    body_send_stream.close()

        app_runner = _AwaitableRunner(self.app(scope, receive, send))

        try:
            await app_runner.run_until(response_started.wait)
            if app_runner.finished:
                await app_runner.run_to_completion()
        except Exception:  # noqa: PIE786
            if self.raise_app_exceptions:
                raise

            response_complete.set()
            if status_code is None:
                status_code = 500
            if response_headers is None:
                response_headers = {}

        assert status_code is not None
        assert response_headers is not None

        stream = ASGIResponseStream(
            body_receive_stream,
            self.raise_app_exceptions,
            response_complete,
            app_runner,
        )

        return Response(status_code, headers=response_headers, stream=stream)
