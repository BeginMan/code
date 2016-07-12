# coding=utf-8
"""
非阻塞socket I/O 事件循环

典型的app就是使用一个`IOLoop`对象在`IOLoop.instance`单例中.
`IOLoop.start`方法通常在``main()``函数后被调用。
非典型的应用则是使用多个`IOLoop`, 例如每个线程或每个`unittest`单元测试对应一个`IOLoop`

除了I/O事件之外, `IOLoop`也可基于时间事件调度。
`IOLoop.add_timeout` 是`time.sleep`的非阻塞版本。
"""

from __future__ import absolute_import, division, print_function, with_statement

import datetime
import errno
import functools
import heapq
import itertools
import logging
import numbers
import os
import select
import sys
import threading
import time
import traceback
import math

from tornados.concurrent import TracebackFuture, is_future
from tornados.log import app_log, gen_log
from tornados import stack_context
from tornados.util import Configurable, errno_from_exception, timedelta_to_seconds

try:
    import signal
except ImportError:
    signal = None


try:
    import thread       # py2
except ImportError:
    import _thread as thread    # py3

from tornado.platform.auto import set_close_exec, Waker

_POLL_TIMEOUT = 3600.0


class TimeoueError(Exception):
    pass


class IOLoop(Configurable):
    """
    水平触发的I/O loop
    ``epoll`` or ``kqueue``, ``select``的调度
    """
    # epoll 常量
    _EPOLLIN = 0x001
    _EPOLLPRI = 0x002
    _EPOLLOUT = 0x004
    _EPOLLERR = 0x008
    _EPOLLHUP = 0x010
    _EPOLLRDHUP = 0x2000
    _EPOLLONESHOT = (1 << 30)
    _EPOLLET = (1 << 31)

    # Our events map exactly to the epoll events
    NONE = 0
    READ = _EPOLLIN
    WRITE = _EPOLLOUT
    ERROR = _EPOLLERR | _EPOLLHUP

    # 创建全局 IOLoop实例的全局锁
    _instance_lock = threading.Lock()

    _current = threading.local()    # 线程变量,生命周期是在线程里
                                    # 在当前线程保存一个全局值，并且各自线程互不干扰

    @staticmethod
    def instance():
        """
        返回一个全局`IOLoop`实例
        通过线程锁实现线程同步保证多线程环境下的单例
        """
        if not hasattr(IOLoop, "_instance"):
            with IOLoop._instance_lock:
                if not hasattr(IOLoop, "_instance"):
                    # double check
                    IOLoop._instance = IOLoop()
        return IOLoop._instance

    @staticmethod
    def initialized():
        """返回true如果单例已经存在"""
        return hasattr(IOLoop, "_instance")

    @staticmethod
    def install(self):
        assert not IOLoop.initialized()
        IOLoop._instance = self

    @staticmethod
    def clear_instance():
        """清除全局`IOLoop`实例"""
        if hasattr(IOLoop, '_instance'):
            del IOLoop._instance

    @staticmethod
    def current(instance=True):
        """返回当前线程的`IOLoop`

        当构造一个异步对象时使用current方法
        当在主线程和其他线程通信时使用instance方法
        """
        current = getattr(IOLoop, 'instance', None)
        if not current and instance:
            return IOLoop.instance()
        return current

    def make_current(self):
        """使得这个“IOLoop”为当前线程。

        """
        # 这里作用在线程变量_current上
        IOLoop._current.instance = self

    @staticmethod
    def clear_current():
        # 清除线程变量_current.instance
        IOLoop._current.instance = None

    @classmethod
    def configurable_base(cls):
        """实现父类configurable_base的抽象方法"""
        return IOLoop

    @classmethod
    def configurable_default(cls):
        """实现父类configurable_default的抽象方法
        返回epoll,kqueue,或 select系统事件对象
        """
        if hasattr(select, "epoll"):
            from tornados.platform.epoll import EPollIOLoop
            return EPollIOLoop
        if hasattr(select, "kqueue"):
            # Python 2.6+ on BSD or Mac
            from tornados.platform.kqueue import KQueueIOLoop
            return KQueueIOLoop
        from tornados.platform.select import SelectIOLoop
        return SelectIOLoop

    def initialize(self, make_current=None):
        """继承父类Configurable时, 执行父类__new___构造方法

        该父类执行逻辑包括initialize方法的执行.
        这里子类实现initialize具体的逻辑: 构造当前IOLoop对象
        """
        if make_current is None:
            if IOLoop.current(instance=False) is None:
                self.make_current()
        elif make_current:
            if IOLoop.current(instance=False) is not None:
                raise RuntimeError("current IOLoop already exists")
            self.make_current()

    def close(self, all_fds=False):
        """关闭`IOLoop`,释放所有占用的资源

        这是一个抽象方法。

        如果`all_fds`为true, 所有注册在IOLoop上的文件描述符都将被关闭(
        不仅仅是`IOLoop`自身的创建)

        许多应用程序将只使用一个`IOLoop`在整个进程的生命周期中。这种情况下关闭`IOLoop`
        没有必要因为一切都会进程退出时清理干净。
        `IOLoop.close`主要应用场景是提供接单元测试等创建和摧毁大量IOLoops

        `IOLoop`必须完全stop后才可以被关闭,这意味着`IOLoop.stop()`方法必须被调用,
        且`IOLoop.start()`方法必须被允许返回在尝试调用`IOLoop.close()`之前
        """
        raise NotImplementedError()

    def add_handler(self, fd, handler, events):
        """fd注册handler

        `fd`:任何有`fileno()`的file-like对象
        `events`: 就是上面的READ,WRITE,ERROR

        当事件触发后, `handler(fd, events)` 将被执行
        """
        raise NotImplementedError()

    def update_handler(self, fd, events):
        """更改监听的`fd`事件"""
        raise NotImplementedError()

    def remove_handler(self, fd):
        """停止对`fd`的事件监听"""
        raise NotImplementedError()

    def set_blocking_signal_threshold(self, seconds, action):
        """如果`IOLoop`被阻塞了``s``秒后则发送一个信号"""
        raise NotImplementedError()

    def set_blocking_log_threshold(self, seconds):
        """`IOLoop`被阻塞`s`秒后用日志记录一个堆栈跟踪"""
        self.set_blocking_signal_threshold(seconds, self.log_stack)

    def log_stack(self, signal, frame):
        gen_log.warning('IOLoop blocked for %f seconds in\n%s',
                        self._blocking_signal_threshold,
                        ''.join(traceback.format_stack(frame)))

    def start(self):
        """开始 I/O loop.

        The loop will run until one of the callbacks calls `stop()`, which
        will make the loop stop after the current event iteration completes.
        """
        raise NotImplementedError()

    def _setup_logging(self):
        """The IOLoop catches and logs exceptions, so it's
        important that log output be visible.  However, python's
        default behavior for non-root loggers (prior to python
        3.2) is to print an unhelpful "no handlers could be
        found" message rather than the actual log entry, so we
        must explicitly configure logging if we've made it this
        far without anything.

        This method should be called from start() in subclasses.
        """
        if not any([logging.getLogger().handlers,
                    logging.getLogger('tornado').handlers,
                    logging.getLogger('tornado.application').handlers]):
            logging.basicConfig()

    def stop(self):
        """停止ioloop"""
        raise NotImplementedError()

    def run_sync(self, func, timeout=None):
        """开始IOLoop,执行给定的函数,然后stop ioloop

        给定的函数必须返回一个 yieldable 对象或``None``.
        如果返回 yieldable 对象, `IOLoop` 将执行直到yieldable 被 resolved (and
        `run_sync()` will return the yieldable's result).
        如果触发异常, `IOLoop`将stop 且 异常会被重新抛给调用者

        `timeout`函数执行的超时时间,到期则触发`TimeoutError`异常.

        该方法结合`tornado.gen.coroutine`进行 异步调用 在``main()`` 函数中::

            @gen.coroutine
            def main():
                # do stuff...

            if __name__ == '__main__':
                IOLoop.current().run_sync(main)
        """
        future_cell = [None]

        def run():
            """真正的回调执行逻辑"""
            try:
                result = func()
                if result is not None:
                    from tornados.gen import convert_yielded     # todo:
                    result = convert_yielded(result)
            except Exception:
                future_cell[0] = TracebackFuture()              # todo:
                future_cell[0].set_exc_info(sys.exc_info())
            else:
                if is_future(result):
                    future_cell[0] = result
                else:
                    future_cell[0] = TracebackFuture()
                    future_cell[0].set_result(result)

            self.add_future(future_cell[0], lambda future: self.stop())

        self.add_callback(run)      # run添加到回调
        if timeout is not None:     # 超时处理, 在add_timeout指定时间来 stop()
            timeout_handle = self.add_timeout(self.time() + timeout, self.stop)
        # 开始io loop
        self.start()

        # 执行结果的处理
        if timeout is not None:         # 未超时, 则移除超时操作 timeout_handle
            self.remove_timeout(timeout_handle)
        if not future_cell[0].done():   # 超时抛出TimeoutError异常
            raise TimeoueError('Operation timed out after %s seconds' % timeout)
        return future_cell[0].result()


    def time(self):
        """根据`IOLoop`的时钟, 返回当前时间

        默认`IOLoop`'的时间函数是:`time.time`. 然而,它可被配置使用 e.g. `time.monotonic`.
        调用`add_timeout`方法传递一个数值参数来取代`datetime.timedelta`
        """
        return time.time()

    def add_timeout(self, deadline, callback, *args, **kwargs):
        """从I/O循环中在``deadline``时间内执行``callback``

        返回一个不透明的句柄传递给“remove_timeout”取消。

        ``deadline`` 截止日期, 可能是一个数字表示一个时间(同`IOLoop.time`,或者是
        `datetime.timedelta` 对象。从tornado 4.0后, `call_later`是一个更方便的替代
        以来相对情况下并非如此需要一个timedelta对象。

        `add_timeout`不是线程安全的,在其他线程中调用`add_timeout`,取而代之的是add_callback方法
        """
        if isinstance(deadline, numbers.Real):
            return self.call_at(deadline, callback, *args, **kwargs)
        elif isinstance(deadline, datetime.timedelta):
            return self.call_at(self.time() + timedelta_to_seconds(deadline),
                                callback, *args, **kwargs)
        else:
            raise TypeError("Unsupported deadline %r" % deadline)

    def call_later(self, delay, callback, *args, **kwargs):
        """在``delay`` 秒后执行``callback``

        返回一个不透明的(opaque) handle 可传递给`remove_timeout`来取消
        注意,不像`asyncio` 的同名方法, 返回对象没有 ``cancel()`` 方法.
        """
        return self.call_at(self.time() + delay, callback, *args, **kwargs)

    def call_at(self, when, callback, *args, **kwargs):
        """在指定的绝对时间(`when`)内执行``callback``

        ``when`` 必须是同`IOLoop.time`一样的数字

        返回一个不透明的(opaque) handle 可传递给`remove_timeout`来取消
        注意,不像`asyncio` 的同名方法, 返回对象没有 ``cancel()`` 方法.
        """
        return self.add_timeout(when, callback, *args, **kwargs)

    def remove_timeout(self, timeout):
        """取消一个等待超时

        这个函数就是用来移除上面通过add_timeout注册的callback函数。
        """
        raise NotImplementedError()

    def add_callback(self, callback, *args, **kwargs):
        """在下一个I/O循环迭代中调用给定的callback函数

        在ioloop开启后执行的回调函数callback，args和*kwargs都是这个回调函数的参数。
        一般我们的server都是单进程单线程的，即使是多线程，那么这个函数也是安全的。

        可以在任何时候从任何线程调用这个方法,除了信号处理程序外。
        注意这是**唯一** 一个`IOLoop`中线程安全的方法。
        其他所有与“IOLoop”的交互必须在IOLoop线程中完成。

        `add_callback()` may be used to transfer
        control from other threads to the `IOLoop`'s thread.

        To add a callback from a signal handler, see
        `add_callback_from_signal`.
        """
        raise NotImplementedError()

    def add_callback_from_signal(self, callback, *args, **kwargs):
        """Calls the given callback on the next I/O loop iteration.

        Safe for use from a Python signal handler; should not be used
        otherwise.

        Callbacks added with this method will be run without any
        `.stack_context`, to avoid picking up the context of the function
        that was interrupted by the signal.
        """
        raise NotImplementedError()

    def spawn_callback(self, callback, *args, **kwargs):
        """Calls the given callback on the next IOLoop iteration.

        Unlike all other callback-related methods on IOLoop,
        ``spawn_callback`` does not associate the callback with its caller's
        ``stack_context``, so it is suitable for fire-and-forget callbacks
        that should not interfere with the caller.

        .. versionadded:: 4.0
        """
        with stack_context.NullContext():
            self.add_callback(callback, *args, **kwargs)

    def add_future(self, future, callback):
        """当给定的 `.Future` 已经完成, 在``IOLoop``中安排一个回调callback.

        callback只需要一个参数就是`.Future`.
        """
        assert is_future(future)
        callback = stack_context.wrap(callback)
        future.add_done_callback(
            lambda future: self.add_callback(callback, future))