# coding=utf-8
"""``tornado.gen`` 是一个基于生成器(generator-based)接口使之更加容易作用在异步环境
使用``gen``模块的代码应该是异步的。

它应该是一个单独的生成器,而不是一系列函数。
也就是说解放了callback回调的噩梦, 使之以同步的方式写异步代码。

For example, the following asynchronous handler:

.. testcode::

    class AsyncHandler(RequestHandler):
        @asynchronous
        def get(self):
            http_client = AsyncHTTPClient()
            http_client.fetch("http://example.com",
                              callback=self.on_fetch)

        def on_fetch(self, response):
            do_something_with_response(response)
            self.render("template.html")

.. testoutput::
   :hide:

could be written with ``gen`` as:

.. testcode::

    class GenAsyncHandler(RequestHandler):
        @gen.coroutine
        def get(self):
            http_client = AsyncHTTPClient()
            response = yield http_client.fetch("http://example.com")
            do_something_with_response(response)
            self.render("template.html")

.. testoutput::
   :hide:

Tornado中多数异步函数返回`.Future`;
产生(yielding)这个对象返回它 `~.Future.result`.

你也可以 yield a list or dict of ``Futures``,将开始在**同一时间并行执行**;
当都完成后返回 a list or dict of results。

.. testcode::

    @gen.coroutine
    def get(self):
        http_client = AsyncHTTPClient()
        response1, response2 = yield [http_client.fetch(url1),
                                      http_client.fetch(url2)]
        response_dict = yield dict(response3=http_client.fetch(url3),
                                   response4=http_client.fetch(url4))
        response3 = response_dict['response3']
        response4 = response_dict['response4']

.. testoutput::
   :hide:

如果 `~functools.singledispatch` 库可用 (Python 3.4 标准库,
<https://pypi.python.org/pypi/singledispatch>`_ package on older
versions), 其他类型的对象可能产生.
Tornado 包括支持 ``asyncio.Future`` 和 Twisted's ``Deferred`` 类,当
``tornado.platform.asyncio`` 和 ``tornado.platform.twisted`` 被导入时.
`convert_yielded` 函数进行扩展

.. versionchanged:: 3.2
   Dict support added.

.. versionchanged:: 4.1
   Support added for yielding ``asyncio`` Futures and Twisted Deferreds
   via ``singledispatch``.

"""

from __future__ import absolute_import, division, print_function, with_statement

import collections
import functools
import itertools
import os
import sys
import textwrap
import types

from tornados.concurrent import Future, TracebackFuture, is_future, chain_future
from tornados.ioloop import IOLoop
from tornados.log import app_log
from tornados import stack_context
from tornados.util import PY3, raise_exc_info

try:
    try:
        # py34+
        from functools import singledispatch  # type: ignore
    except ImportError:
        from singledispatch import singledispatch  # backport
except ImportError:
    # In most cases, singledispatch is required (to avoid
    # difficult-to-diagnose problems in which the functionality
    # available differs depending on which invisble packages are
    # installed). However, in Google App Engine third-party
    # dependencies are more trouble so we allow this module to be
    # imported without it.
    if 'APPENGINE_RUNTIME' not in os.environ:
        raise
    singledispatch = None

try:
    try:
        # py35+
        from collections.abc import Generator as GeneratorType  # type: ignore
    except ImportError:
        from backports_abc import Generator as GeneratorType  # type: ignore

    try:
        # py35+
        from inspect import isawaitable  # type: ignore
    except ImportError:
        from backports_abc import isawaitable
except ImportError:
    if 'APPENGINE_RUNTIME' not in os.environ:
        raise
    from types import GeneratorType

    def isawaitable(x):  # type: ignore
        return False

if PY3:
    import builtins
else:
    import __builtin__ as builtins


class KeyReuseError(Exception):
    pass


class UnknownKeyError(Exception):
    pass


class LeakedCallbackError(Exception):
    pass


class BadYieldError(Exception):
    pass


class ReturnValueIgnoredError(Exception):
    pass


class TimeoutError(Exception):
    """Exception raised by ``with_timeout``."""


def _value_from_stopiteration(e):
    """从StopIteration取值"""
    try:
        # 在py33后 StopIteration 有value属性
        # So does our Return class.
        return e.value
    except AttributeError:
        pass
    try:
        # Cython backports(补丁) 协程功能通过 putting the value in e.args[0].
        return e.args[0]
    except (AttributeError, IndexError):
        return None


def engine(func):
    """Callback-oriented decorator for asynchronous generators.

    This is an older interface; for new code that does not need to be
    compatible with versions of Tornado older than 3.0 the
    `coroutine` decorator is recommended instead.

    This decorator is similar to `coroutine`, except it does not
    return a `.Future` and the ``callback`` argument is not treated
    specially.

    In most cases, functions decorated with `engine` should take
    a ``callback`` argument and invoke it with their result when
    they are finished.  One notable exception is the
    `~tornado.web.RequestHandler` :ref:`HTTP verb methods <verbs>`,
    which use ``self.finish()`` in place of a callback argument.
    """
    func = _make_coroutine_wrapper(func, replace_callback=False)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        future = func(*args, **kwargs)

        def final_callback(future):
            if future.result() is not None:
                raise ReturnValueIgnoredError(
                    "@gen.engine functions cannot return values: %r" %
                    (future.result(),))
        # The engine interface doesn't give us any way to return
        # errors but to raise them into the stack context.
        # Save the stack context here to use when the Future has resolved.
        future.add_done_callback(stack_context.wrap(final_callback))
    return wrapper


def coroutine(func, replace_callback=True):
    """Decorator for asynchronous generators.

    coroutine 内部委托 `_make_coroutine_wrapper` 完成具体功能
    `replace_callback` 没有使用的;返回一个 Future 实例对象。

    Coroutines "return" 通过触发特殊的异常`Return(value) <Return>`.
    Python 3.3+, 可直接``return value`` 语句 (py3.3之前生成器是不可return values的)。

    从调用者的角度来看, `@gen.coroutine` 类似于`@return_future`和 `@gen.engine`的组合。

    .. warning::

       当异常发生在coroutine内部,异常信息将存储在`.Future` object.
       你必须检查`.Future`的结果或忽略异常.这意味着如果从另一个协程yielding function, 使用像
       `.IOLoop.run_sync` 的顶级调用, 或传递`.Future` 到`.IOLoop.add_future`中.

    """
    return _make_coroutine_wrapper(func, replace_callback=True)


def _make_coroutine_wrapper(func, replace_callback):
    """``@gen.coroutine`` 和 ``@gen.engine``的内部运作方式.

    `@gen.coroutine` 与 `@gen.engine` 的功能非常相似，
    差别就在于二者对被装饰方法参数中的 “callback” 参数处理不一样以及具有不同的返回值。
    `@gen.coroutine` 装饰的方法执行后返回 `Future` 对象并且会将方法参数中的 “callback”
    加入到 Future 完成后的回调列表中；
    `@gen.engine` 装饰的方法执行后没有返回值
    （注：实际上如果被装饰方法有返回值，会抛出 ReturnValueIgnoredError 异常）

    所以，通过 @gen.engine 装饰的方法没有返回值，方法必须自己在异步调用完成后调用 “callback” 来执行回调动作，
    而通过 @gen.coroutine 装饰的方法则可以直接返回执行结果，然后由 gen 模块负责将结果传递给 “callback” 来执行回调。

    注： 从调用者的角度来看 @gen.coroutine 可以视为 @tornado.concurrent.return_future 与 @gen.engine 的组合。
    """
    # On Python 3.5, set the coroutine flag on our generator, to allow it
    # to be used with 'await'.
    if hasattr(types, 'coroutine'):
        func = types.coroutine(func)

    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        future = TracebackFuture()          # Future实例
        # 处理 “callback”，忽略或者将其加入到 Future 的完成回调列表中。
        if replace_callback and 'callback' in kwargs:
            callback = kwargs.pop('callback')
            IOLoop.current().add_future(
                future, lambda future: callback(future.result()))

        try:
            result = func(*args, **kwargs)
        except (Return, StopIteration) as e:
            # 在python 3.3前，generator不能直接通过return返回,
            # return 被视为 raise StopIteration()，
            # return <something> 被视为 raise StopIteration(<something>)。
            # 在 gen 模块中，特别定义了 Return 类型用于返回值：raise gen.Return(something>)
            result = _value_from_stopiteration(e)
        except Exception:
            # 发生异常，异常被写入 future（将会被设置为完成状态），结束调用，返回 future
            future.set_exc_info(sys.exc_info())
            return future
        else:
            if isinstance(result, GeneratorType):
                # 通过检查 result 是否为 GeneratorType 来选择是否创建 coroutine
                # 对于同步情况直接 future.set_result(result) 返回，避免创建 coroutine 而
                # 造成的性能损失.
                try:
                    # 通过 next 启动 generator ，启动前记录上下文，启动后对上下文进行一致性检查
                    orig_stack_contexts = stack_context._state.contexts
                    yielded = next(result)
                    print(yielded)
                    if stack_context._state.contexts is not orig_stack_contexts:
                        yielded = TracebackFuture()
                        yielded.set_exception(
                            stack_context.StackContextInconsistentError(
                                'stack_context inconsistency (probably caused '
                                'by yield within a "with StackContext" block)'))
                except (StopIteration, Return) as e:
                    future.set_result(_value_from_stopiteration(e))
                except Exception:
                    future.set_exc_info(sys.exc_info())
                else:
                    print('run....')
                    Runner(result, future, yielded)
                try:
                    print(future)
                    return future
                finally:
                    # generator.next() 抛出异常失败后， future 的 exc_info
                    # 中会包含当前栈帧的引用，栈帧中也有对 future 的引用，这样导致一个环，必须
                    # 要在下一次 full GC 时才能回收内存。返回 future 后将 future 设置为 None
                    # 可以优化内存。（注：需要 full GC 是与 python 的垃圾回收实现采用引用计数
                    # 为主，标记-清除和分代机制为辅相关。python 采用引用计数来立刻释放可以释放
                    # 的内存，然后用标记-清除的方法来清除循环引用的不可达对象。）

                    future = None
        # 同步情况下，不需要创建 coroutine，直接返回 future。
        future.set_result(result)
        print(future)
        return future
    return wrapper


class Return(Exception):
    """
    从`coroutine`返回值的特殊异常

        @gen.coroutine
        def fetch_json(url):
            response = yield AsyncHTTPClient().fetch(url)
            raise gen.Return(json_decode(response.body))
    """
    def __init__(self, value=None):
        super(Return, self).__init__()
        self.value = value
        # Cython recognizes subclasses of StopIteration with a .args tuple.
        self.args = (value,)


class WaitIterator(object):
    """Provides an iterator to yield the results of futures as they finish.

    Yielding a set of futures like this:

    ``results = yield [future1, future2]``

    pauses the coroutine until both ``future1`` and ``future2``
    return, and then restarts the coroutine with the results of both
    futures. If either future is an exception, the expression will
    raise that exception and all the results will be lost.

    If you need to get the result of each future as soon as possible,
    or if you need the result of some futures even if others produce
    errors, you can use ``WaitIterator``::

      wait_iterator = gen.WaitIterator(future1, future2)
      while not wait_iterator.done():
          try:
              result = yield wait_iterator.next()
          except Exception as e:
              print("Error {} from {}".format(e, wait_iterator.current_future))
          else:
              print("Result {} received from {} at {}".format(
                  result, wait_iterator.current_future,
                  wait_iterator.current_index))

    Because results are returned as soon as they are available the
    output from the iterator *will not be in the same order as the
    input arguments*. If you need to know which future produced the
    current result, you can use the attributes
    ``WaitIterator.current_future``, or ``WaitIterator.current_index``
    to get the index of the future from the input list. (if keyword
    arguments were used in the construction of the `WaitIterator`,
    ``current_index`` will use the corresponding keyword).

    On Python 3.5, `WaitIterator` implements the async iterator
    protocol, so it can be used with the ``async for`` statement (note
    that in this version the entire iteration is aborted if any value
    raises an exception, while the previous example can continue past
    individual errors)::

      async for result in gen.WaitIterator(future1, future2):
          print("Result {} received from {} at {}".format(
              result, wait_iterator.current_future,
              wait_iterator.current_index))

    .. versionadded:: 4.1

    .. versionchanged:: 4.3
       Added ``async for`` support in Python 3.5.

    """
    def __init__(self, *args, **kwargs):
        if args and kwargs:
            raise ValueError(
                "You must provide args or kwargs, not both")

        if kwargs:
            self._unfinished = dict((f, k) for (k, f) in kwargs.items())
            futures = list(kwargs.values())
        else:
            self._unfinished = dict((f, i) for (i, f) in enumerate(args))
            futures = args

        self._finished = collections.deque()
        self.current_index = self.current_future = None
        self._running_future = None

        for future in futures:
            future.add_done_callback(self._done_callback)

    def done(self):
        """Returns True if this iterator has no more results."""
        if self._finished or self._unfinished:
            return False
        # Clear the 'current' values when iteration is done.
        self.current_index = self.current_future = None
        return True

    def next(self):
        """Returns a `.Future` that will yield the next available result.

        Note that this `.Future` will not be the same object as any of
        the inputs.
        """
        self._running_future = TracebackFuture()

        if self._finished:
            self._return_result(self._finished.popleft())

        return self._running_future

    def _done_callback(self, done):
        if self._running_future and not self._running_future.done():
            self._return_result(done)
        else:
            self._finished.append(done)

    def _return_result(self, done):
        """Called set the returned future's state that of the future
        we yielded, and set the current future for the iterator.
        """
        chain_future(done, self._running_future)

        self.current_future = done
        self.current_index = self._unfinished.pop(done)

    @coroutine
    def __aiter__(self):
        raise Return(self)

    def __anext__(self):
        if self.done():
            # Lookup by name to silence pyflakes on older versions.
            raise getattr(builtins, 'StopAsyncIteration')()
        return self.next()


class YieldPoint(object):
    """Base class for objects that may be yielded from the generator.

    .. deprecated:: 4.0
       Use `Futures <.Future>` instead.
    """
    def start(self, runner):
        """Called by the runner after the generator has yielded.

        No other methods will be called on this object before ``start``.
        """
        raise NotImplementedError()

    def is_ready(self):
        """Called by the runner to determine whether to resume the generator.

        Returns a boolean; may be called more than once.
        """
        raise NotImplementedError()

    def get_result(self):
        """Returns the value to use as the result of the yield expression.

        This method will only be called once, and only after `is_ready`
        has returned true.
        """
        raise NotImplementedError()


class Callback(YieldPoint):
    """Returns a callable object that will allow a matching `Wait` to proceed.

    The key may be any value suitable for use as a dictionary key, and is
    used to match ``Callbacks`` to their corresponding ``Waits``.  The key
    must be unique among outstanding callbacks within a single run of the
    generator function, but may be reused across different runs of the same
    function (so constants generally work fine).

    The callback may be called with zero or one arguments; if an argument
    is given it will be returned by `Wait`.

    .. deprecated:: 4.0
       Use `Futures <.Future>` instead.
    """
    def __init__(self, key):
        self.key = key

    def start(self, runner):
        self.runner = runner
        runner.register_callback(self.key)

    def is_ready(self):
        return True

    def get_result(self):
        return self.runner.result_callback(self.key)


class Wait(YieldPoint):
    """Returns the argument passed to the result of a previous `Callback`.

    .. deprecated:: 4.0
       Use `Futures <.Future>` instead.
    """
    def __init__(self, key):
        self.key = key

    def start(self, runner):
        self.runner = runner

    def is_ready(self):
        return self.runner.is_ready(self.key)

    def get_result(self):
        return self.runner.pop_result(self.key)


class WaitAll(YieldPoint):
    """Returns the results of multiple previous `Callbacks <Callback>`.

    The argument is a sequence of `Callback` keys, and the result is
    a list of results in the same order.

    `WaitAll` is equivalent to yielding a list of `Wait` objects.

    .. deprecated:: 4.0
       Use `Futures <.Future>` instead.
    """
    def __init__(self, keys):
        self.keys = keys

    def start(self, runner):
        self.runner = runner

    def is_ready(self):
        return all(self.runner.is_ready(key) for key in self.keys)

    def get_result(self):
        return [self.runner.pop_result(key) for key in self.keys]


def Task(func, *args, **kwargs):
    """Adapts a callback-based asynchronous function for use in coroutines.

    Takes a function (and optional additional arguments) and runs it with
    those arguments plus a ``callback`` keyword argument.  The argument passed
    to the callback is returned as the result of the yield expression.

    .. versionchanged:: 4.0
       ``gen.Task`` is now a function that returns a `.Future`, instead of
       a subclass of `YieldPoint`.  It still behaves the same way when
       yielded.
    """
    future = Future()

    def handle_exception(typ, value, tb):
        if future.done():
            return False
        future.set_exc_info((typ, value, tb))
        return True

    def set_result(result):
        if future.done():
            return
        future.set_result(result)
    with stack_context.ExceptionStackContext(handle_exception):
        func(*args, callback=_argument_adapter(set_result), **kwargs)
    return future


class YieldFuture(YieldPoint):
    def __init__(self, future, io_loop=None):
        """Adapts a `.Future` to the `YieldPoint` interface.

        .. versionchanged:: 4.1
           The ``io_loop`` argument is deprecated.
        """
        self.future = future
        self.io_loop = io_loop or IOLoop.current()

    def start(self, runner):
        if not self.future.done():
            self.runner = runner
            self.key = object()
            runner.register_callback(self.key)
            self.io_loop.add_future(self.future, runner.result_callback(self.key))
        else:
            self.runner = None
            self.result_fn = self.future.result

    def is_ready(self):
        if self.runner is not None:
            return self.runner.is_ready(self.key)
        else:
            return True

    def get_result(self):
        if self.runner is not None:
            return self.runner.pop_result(self.key).result()
        else:
            return self.result_fn()


def _contains_yieldpoint(children):
    """Returns True if ``children`` contains any YieldPoints.

    ``children`` may be a dict or a list, as used by `MultiYieldPoint`
    and `multi_future`.
    """
    if isinstance(children, dict):
        return any(isinstance(i, YieldPoint) for i in children.values())
    if isinstance(children, list):
        return any(isinstance(i, YieldPoint) for i in children)
    return False


def multi(children, quiet_exceptions=()):
    """Runs multiple asynchronous operations in parallel.

    ``children`` may either be a list or a dict whose values are
    yieldable objects. ``multi()`` returns a new yieldable
    object that resolves to a parallel structure containing their
    results. If ``children`` is a list, the result is a list of
    results in the same order; if it is a dict, the result is a dict
    with the same keys.

    That is, ``results = yield multi(list_of_futures)`` is equivalent
    to::

        results = []
        for future in list_of_futures:
            results.append(yield future)

    If any children raise exceptions, ``multi()`` will raise the first
    one. All others will be logged, unless they are of types
    contained in the ``quiet_exceptions`` argument.

    If any of the inputs are `YieldPoints <YieldPoint>`, the returned
    yieldable object is a `YieldPoint`. Otherwise, returns a `.Future`.
    This means that the result of `multi` can be used in a native
    coroutine if and only if all of its children can be.

    In a ``yield``-based coroutine, it is not normally necessary to
    call this function directly, since the coroutine runner will
    do it automatically when a list or dict is yielded. However,
    it is necessary in ``await``-based coroutines, or to pass
    the ``quiet_exceptions`` argument.

    This function is available under the names ``multi()`` and ``Multi()``
    for historical reasons.

    .. versionchanged:: 4.2
       If multiple yieldables fail, any exceptions after the first
       (which is raised) will be logged. Added the ``quiet_exceptions``
       argument to suppress this logging for selected exception types.

    .. versionchanged:: 4.3
       Replaced the class ``Multi`` and the function ``multi_future``
       with a unified function ``multi``. Added support for yieldables
       other than `YieldPoint` and `.Future`.

    """
    if _contains_yieldpoint(children):
        return MultiYieldPoint(children, quiet_exceptions=quiet_exceptions)
    else:
        return multi_future(children, quiet_exceptions=quiet_exceptions)

Multi = multi


class MultiYieldPoint(YieldPoint):
    """Runs multiple asynchronous operations in parallel.

    This class is similar to `multi`, but it always creates a stack
    context even when no children require it. It is not compatible with
    native coroutines.

    .. versionchanged:: 4.2
       If multiple ``YieldPoints`` fail, any exceptions after the first
       (which is raised) will be logged. Added the ``quiet_exceptions``
       argument to suppress this logging for selected exception types.

    .. versionchanged:: 4.3
       Renamed from ``Multi`` to ``MultiYieldPoint``. The name ``Multi``
       remains as an alias for the equivalent `multi` function.

    .. deprecated:: 4.3
       Use `multi` instead.
    """
    def __init__(self, children, quiet_exceptions=()):
        self.keys = None
        if isinstance(children, dict):
            self.keys = list(children.keys())
            children = children.values()
        self.children = []
        for i in children:
            if not isinstance(i, YieldPoint):
                i = convert_yielded(i)
            if is_future(i):
                i = YieldFuture(i)
            self.children.append(i)
        assert all(isinstance(i, YieldPoint) for i in self.children)
        self.unfinished_children = set(self.children)
        self.quiet_exceptions = quiet_exceptions

    def start(self, runner):
        for i in self.children:
            i.start(runner)

    def is_ready(self):
        finished = list(itertools.takewhile(
            lambda i: i.is_ready(), self.unfinished_children))
        self.unfinished_children.difference_update(finished)
        return not self.unfinished_children

    def get_result(self):
        result_list = []
        exc_info = None
        for f in self.children:
            try:
                result_list.append(f.get_result())
            except Exception as e:
                if exc_info is None:
                    exc_info = sys.exc_info()
                else:
                    if not isinstance(e, self.quiet_exceptions):
                        app_log.error("Multiple exceptions in yield list",
                                      exc_info=True)
        if exc_info is not None:
            raise_exc_info(exc_info)
        if self.keys is not None:
            return dict(zip(self.keys, result_list))
        else:
            return list(result_list)


def multi_future(children, quiet_exceptions=()):
    """Wait for multiple asynchronous futures in parallel.

    This function is similar to `multi`, but does not support
    `YieldPoints <YieldPoint>`.

    .. versionadded:: 4.0

    .. versionchanged:: 4.2
       If multiple ``Futures`` fail, any exceptions after the first (which is
       raised) will be logged. Added the ``quiet_exceptions``
       argument to suppress this logging for selected exception types.

    .. deprecated:: 4.3
       Use `multi` instead.
    """
    if isinstance(children, dict):
        keys = list(children.keys())
        children = children.values()
    else:
        keys = None
    children = list(map(convert_yielded, children))
    assert all(is_future(i) for i in children)
    unfinished_children = set(children)

    future = Future()
    if not children:
        future.set_result({} if keys is not None else [])

    def callback(f):
        unfinished_children.remove(f)
        if not unfinished_children:
            result_list = []
            for f in children:
                try:
                    result_list.append(f.result())
                except Exception as e:
                    if future.done():
                        if not isinstance(e, quiet_exceptions):
                            app_log.error("Multiple exceptions in yield list",
                                          exc_info=True)
                    else:
                        future.set_exc_info(sys.exc_info())
            if not future.done():
                if keys is not None:
                    future.set_result(dict(zip(keys, result_list)))
                else:
                    future.set_result(result_list)

    listening = set()
    for f in children:
        if f not in listening:
            listening.add(f)
            f.add_done_callback(callback)
    return future


def maybe_future(x):
    """Converts ``x`` into a `.Future`.

    If ``x`` is already a `.Future`, it is simply returned; otherwise
    it is wrapped in a new `.Future`.  This is suitable for use as
    ``result = yield gen.maybe_future(f())`` when you don't know whether
    ``f()`` returns a `.Future` or not.

    .. deprecated:: 4.3
       This function only handles ``Futures``, not other yieldable objects.
       Instead of `maybe_future`, check for the non-future result types
       you expect (often just ``None``), and ``yield`` anything unknown.
    """
    if is_future(x):
        return x
    else:
        fut = Future()
        fut.set_result(x)
        return fut


def with_timeout(timeout, future, io_loop=None, quiet_exceptions=()):
    """Wraps a `.Future` (or other yieldable object) in a timeout.

    Raises `TimeoutError` if the input future does not complete before
    ``timeout``, which may be specified in any form allowed by
    `.IOLoop.add_timeout` (i.e. a `datetime.timedelta` or an absolute time
    relative to `.IOLoop.time`)

    If the wrapped `.Future` fails after it has timed out, the exception
    will be logged unless it is of a type contained in ``quiet_exceptions``
    (which may be an exception type or a sequence of types).

    Does not support `YieldPoint` subclasses.

    .. versionadded:: 4.0

    .. versionchanged:: 4.1
       Added the ``quiet_exceptions`` argument and the logging of unhandled
       exceptions.

    .. versionchanged:: 4.4
       Added support for yieldable objects other than `.Future`.
    """
    # TODO: allow YieldPoints in addition to other yieldables?
    # Tricky to do with stack_context semantics.
    #
    # It's tempting to optimize this by cancelling the input future on timeout
    # instead of creating a new one, but A) we can't know if we are the only
    # one waiting on the input future, so cancelling it might disrupt other
    # callers and B) concurrent futures can only be cancelled while they are
    # in the queue, so cancellation cannot reliably bound our waiting time.
    future = convert_yielded(future)
    result = Future()
    chain_future(future, result)
    if io_loop is None:
        io_loop = IOLoop.current()

    def error_callback(future):
        try:
            future.result()
        except Exception as e:
            if not isinstance(e, quiet_exceptions):
                app_log.error("Exception in Future %r after timeout",
                              future, exc_info=True)

    def timeout_callback():
        result.set_exception(TimeoutError("Timeout"))
        # In case the wrapped future goes on to fail, log it.
        future.add_done_callback(error_callback)
    timeout_handle = io_loop.add_timeout(
        timeout, timeout_callback)
    if isinstance(future, Future):
        # We know this future will resolve on the IOLoop, so we don't
        # need the extra thread-safety of IOLoop.add_future (and we also
        # don't care about StackContext here.
        future.add_done_callback(
            lambda future: io_loop.remove_timeout(timeout_handle))
    else:
        # concurrent.futures.Futures may resolve on any thread, so we
        # need to route them back to the IOLoop.
        io_loop.add_future(
            future, lambda future: io_loop.remove_timeout(timeout_handle))
    return result


def sleep(duration):
    """Return a `.Future` that resolves after the given number of seconds.

    When used with ``yield`` in a coroutine, this is a non-blocking
    analogue to `time.sleep` (which should not be used in coroutines
    because it is blocking)::

        yield gen.sleep(0.5)

    Note that calling this function on its own does nothing; you must
    wait on the `.Future` it returns (usually by yielding it).

    .. versionadded:: 4.1
    """
    f = Future()
    IOLoop.current().call_later(duration, lambda: f.set_result(None))
    return f


_null_future = Future()
_null_future.set_result(None)

moment = Future()
moment.__doc__ = \
    """A special object which may be yielded to allow the IOLoop to run for
one iteration.

This is not needed in normal use but it can be helpful in long-running
coroutines that are likely to yield Futures that are ready instantly.

Usage: ``yield gen.moment``

.. versionadded:: 4.0
"""
moment.set_result(None)


class Runner(object):
    """Internal implementation of `tornado.gen.engine`.

    Maintains information about pending callbacks and their results.

    The results of the generator are stored in ``result_future`` (a
    `.TracebackFuture`)
    """
    def __init__(self, gen, result_future, first_yielded):
        self.gen = gen
        self.result_future = result_future
        self.future = _null_future
        self.yield_point = None
        self.pending_callbacks = None
        self.results = None
        self.running = False
        self.finished = False
        self.had_exception = False
        self.io_loop = IOLoop.current()
        # For efficiency, we do not create a stack context until we
        # reach a YieldPoint (stack contexts are required for the historical
        # semantics of YieldPoints, but not for Futures).  When we have
        # done so, this field will be set and must be called at the end
        # of the coroutine.
        self.stack_context_deactivate = None
        if self.handle_yield(first_yielded):
            self.run()

    def register_callback(self, key):
        """Adds ``key`` to the list of callbacks."""
        if self.pending_callbacks is None:
            # Lazily initialize the old-style YieldPoint data structures.
            self.pending_callbacks = set()
            self.results = {}
        if key in self.pending_callbacks:
            raise KeyReuseError("key %r is already pending" % (key,))
        self.pending_callbacks.add(key)

    def is_ready(self, key):
        """Returns true if a result is available for ``key``."""
        if self.pending_callbacks is None or key not in self.pending_callbacks:
            raise UnknownKeyError("key %r is not pending" % (key,))
        return key in self.results

    def set_result(self, key, result):
        """Sets the result for ``key`` and attempts to resume the generator."""
        self.results[key] = result
        if self.yield_point is not None and self.yield_point.is_ready():
            try:
                self.future.set_result(self.yield_point.get_result())
            except:
                self.future.set_exc_info(sys.exc_info())
            self.yield_point = None
            self.run()

    def pop_result(self, key):
        """Returns the result for ``key`` and unregisters it."""
        self.pending_callbacks.remove(key)
        return self.results.pop(key)

    def run(self):
        """Starts or resumes the generator, running until it reaches a
        yield point that is not ready.
        """
        if self.running or self.finished:
            return
        try:
            self.running = True
            while True:
                future = self.future
                if not future.done():
                    return
                self.future = None
                try:
                    orig_stack_contexts = stack_context._state.contexts
                    exc_info = None

                    try:
                        value = future.result()
                    except Exception:
                        self.had_exception = True
                        exc_info = sys.exc_info()

                    if exc_info is not None:
                        yielded = self.gen.throw(*exc_info)
                        exc_info = None
                    else:
                        yielded = self.gen.send(value)

                    if stack_context._state.contexts is not orig_stack_contexts:
                        self.gen.throw(
                            stack_context.StackContextInconsistentError(
                                'stack_context inconsistency (probably caused '
                                'by yield within a "with StackContext" block)'))
                except (StopIteration, Return) as e:
                    self.finished = True
                    self.future = _null_future
                    if self.pending_callbacks and not self.had_exception:
                        # If we ran cleanly without waiting on all callbacks
                        # raise an error (really more of a warning).  If we
                        # had an exception then some callbacks may have been
                        # orphaned, so skip the check in that case.
                        raise LeakedCallbackError(
                            "finished without waiting for callbacks %r" %
                            self.pending_callbacks)
                    self.result_future.set_result(_value_from_stopiteration(e))
                    self.result_future = None
                    self._deactivate_stack_context()
                    return
                except Exception:
                    self.finished = True
                    self.future = _null_future
                    self.result_future.set_exc_info(sys.exc_info())
                    self.result_future = None
                    self._deactivate_stack_context()
                    return
                if not self.handle_yield(yielded):
                    return
        finally:
            self.running = False

    def handle_yield(self, yielded):
        # Lists containing YieldPoints require stack contexts;
        # other lists are handled in convert_yielded.
        if _contains_yieldpoint(yielded):
            yielded = multi(yielded)

        if isinstance(yielded, YieldPoint):
            # YieldPoints are too closely coupled to the Runner to go
            # through the generic convert_yielded mechanism.
            self.future = TracebackFuture()

            def start_yield_point():
                try:
                    yielded.start(self)
                    if yielded.is_ready():
                        self.future.set_result(
                            yielded.get_result())
                    else:
                        self.yield_point = yielded
                except Exception:
                    self.future = TracebackFuture()
                    self.future.set_exc_info(sys.exc_info())

            if self.stack_context_deactivate is None:
                # Start a stack context if this is the first
                # YieldPoint we've seen.
                with stack_context.ExceptionStackContext(
                        self.handle_exception) as deactivate:
                    self.stack_context_deactivate = deactivate

                    def cb():
                        start_yield_point()
                        self.run()
                    self.io_loop.add_callback(cb)
                    return False
            else:
                start_yield_point()
        else:
            try:
                self.future = convert_yielded(yielded)
            except BadYieldError:
                self.future = TracebackFuture()
                self.future.set_exc_info(sys.exc_info())

        if not self.future.done() or self.future is moment:
            self.io_loop.add_future(
                self.future, lambda f: self.run())
            return False
        return True

    def result_callback(self, key):
        return stack_context.wrap(_argument_adapter(
            functools.partial(self.set_result, key)))

    def handle_exception(self, typ, value, tb):
        if not self.running and not self.finished:
            self.future = TracebackFuture()
            self.future.set_exc_info((typ, value, tb))
            self.run()
            return True
        else:
            return False

    def _deactivate_stack_context(self):
        if self.stack_context_deactivate is not None:
            self.stack_context_deactivate()
            self.stack_context_deactivate = None

Arguments = collections.namedtuple('Arguments', ['args', 'kwargs'])


def _argument_adapter(callback):
    """Returns a function that when invoked runs ``callback`` with one arg.

    If the function returned by this function is called with exactly
    one argument, that argument is passed to ``callback``.  Otherwise
    the args tuple and kwargs dict are wrapped in an `Arguments` object.
    """
    def wrapper(*args, **kwargs):
        if kwargs or len(args) > 1:
            callback(Arguments(args, kwargs))
        elif args:
            callback(args[0])
        else:
            callback(None)
    return wrapper

# Convert Awaitables into Futures. It is unfortunately possible
# to have infinite recursion here if those Awaitables assume that
# we're using a different coroutine runner and yield objects
# we don't understand. If that happens, the solution is to
# register that runner's yieldable objects with convert_yielded.
if sys.version_info >= (3, 3):
    exec(textwrap.dedent("""
    @coroutine
    def _wrap_awaitable(x):
        if hasattr(x, '__await__'):
            x = x.__await__()
        return (yield from x)
    """))
else:
    # Py2-compatible version for use with Cython.
    # Copied from PEP 380.
    @coroutine
    def _wrap_awaitable(x):
        if hasattr(x, '__await__'):
            _i = x.__await__()
        else:
            _i = iter(x)
        try:
            _y = next(_i)
        except StopIteration as _e:
            _r = _value_from_stopiteration(_e)
        else:
            while 1:
                try:
                    _s = yield _y
                except GeneratorExit as _e:
                    try:
                        _m = _i.close
                    except AttributeError:
                        pass
                    else:
                        _m()
                    raise _e
                except BaseException as _e:
                    _x = sys.exc_info()
                    try:
                        _m = _i.throw
                    except AttributeError:
                        raise _e
                    else:
                        try:
                            _y = _m(*_x)
                        except StopIteration as _e:
                            _r = _value_from_stopiteration(_e)
                            break
                else:
                    try:
                        if _s is None:
                            _y = next(_i)
                        else:
                            _y = _i.send(_s)
                    except StopIteration as _e:
                        _r = _value_from_stopiteration(_e)
                        break
        raise Return(_r)


def convert_yielded(yielded):
    """Convert a yielded object into a `.Future`.

    The default implementation accepts lists, dictionaries, and Futures.

    If the `~functools.singledispatch` library is available, this function
    may be extended to support additional types. For example::

        @convert_yielded.register(asyncio.Future)
        def _(asyncio_future):
            return tornado.platform.asyncio.to_tornado_future(asyncio_future)

    .. versionadded:: 4.1
    """
    # Lists and dicts containing YieldPoints were handled earlier.
    if isinstance(yielded, (list, dict)):
        return multi(yielded)
    elif is_future(yielded):
        return yielded
    elif isawaitable(yielded):
        return _wrap_awaitable(yielded)
    else:
        raise BadYieldError("yielded unknown object %r" % (yielded,))

if singledispatch is not None:
    convert_yielded = singledispatch(convert_yielded)

    try:
        # If we can import t.p.asyncio, do it for its side effect
        # (registering asyncio.Future with convert_yielded).
        # It's ugly to do this here, but it prevents a cryptic
        # infinite recursion in _wrap_awaitable.
        # Note that even with this, asyncio integration is unlikely
        # to work unless the application also configures AsyncIOLoop,
        # but at least the error messages in that case are more
        # comprehensible than a stack overflow.
        import tornados.platform.asyncio
    except ImportError:
        pass
    else:
        # Reference the imported module to make pyflakes happy.
        tornados
