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
from tornados.platform.auto import set_close_exec, Waker
from tornados import stack_context
from tornados.util import PY3, Configurable, errno_from_exception, timedelta_to_seconds

try:
    import signal
except ImportError:
    signal = None


if PY3:
    import _thread as thread
else:
    import thread


_POLL_TIMEOUT = 3600.0


class TimeoutError(Exception):
    pass


class IOLoop(Configurable):
    """水平触发的I/O loop
    ``epoll`` or ``kqueue``, ``select``的调度

    Example usage for a simple TCP server:

    .. testcode::

        import errno
        import functools
        import tornado.ioloop
        import socket

        def connection_ready(sock, fd, events):
            while True:
                try:
                    connection, address = sock.accept()
                except socket.error as e:
                    if e.args[0] not in (errno.EWOULDBLOCK, errno.EAGAIN):
                        raise
                    return
                connection.setblocking(0)
                handle_connection(connection, address)

        if __name__ == '__main__':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM, 0)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.setblocking(0)
            sock.bind(("", port))
            sock.listen(128)

            io_loop = tornado.ioloop.IOLoop.current()
            callback = functools.partial(connection_ready, sock)
            io_loop.add_handler(sock.fileno(), callback, io_loop.READ)
            io_loop.start()

    .. testoutput::
       :hide:

    By default, a newly-constructed `IOLoop` becomes the thread's current
    `IOLoop`, unless there already is a current `IOLoop`. This behavior
    can be controlled with the ``make_current`` argument to the `IOLoop`
    constructor: if ``make_current=True``, the new `IOLoop` will always
    try to become current and it raises an error if there is already a
    current instance. If ``make_current=False``, the new `IOLoop` will
    not try to become current.

    .. versionchanged:: 4.2
       Added the ``make_current`` keyword argument to the `IOLoop`
       constructor.
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

    _current = threading.local()        # 线程变量,生命周期是在线程里
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
                    # New instance after double check
                    IOLoop._instance = IOLoop()
        return IOLoop._instance

    @staticmethod
    def initialized():
        """返回true如果单例已经存在"""
        return hasattr(IOLoop, "_instance")

    def install(self):
        """Installs this `IOLoop` object as the singleton instance.

        This is normally not necessary as `instance()` will create
        an `IOLoop` on demand, but you may want to call `install` to use
        a custom subclass of `IOLoop`.

        When using an `IOLoop` subclass, `install` must be called prior
        to creating any objects that implicitly create their own
        `IOLoop` (e.g., :class:`tornado.httpclient.AsyncHTTPClient`).
        """
        assert not IOLoop.initialized()
        IOLoop._instance = self

    @staticmethod
    def clear_instance():
        """Clear the global `IOLoop` instance.

        .. versionadded:: 4.0
        """
        if hasattr(IOLoop, "_instance"):
            del IOLoop._instance

    @staticmethod
    def current(instance=True):
        """返回当前线程的`IOLoop`

        当构造一个异步对象时使用current方法
        当在主线程和其他线程通信时使用instance方法
        """
        current = getattr(IOLoop._current, "instance", None)
        if current is None and instance:
            return IOLoop.instance()
        return current

    def make_current(self):
        """Makes this the `IOLoop` for the current thread.

        An `IOLoop` automatically becomes current for its thread
        when it is started, but it is sometimes useful to call
        `make_current` explicitly before starting the `IOLoop`,
        so that code run at startup time can find the right
        instance.

        .. versionchanged:: 4.1
           An `IOLoop` created while there is no current `IOLoop`
           will automatically become current.
        """
        IOLoop._current.instance = self

    @staticmethod
    def clear_current():
        IOLoop._current.instance = None

    @classmethod
    def configurable_base(cls):
        return IOLoop

    @classmethod
    def configurable_default(cls):
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
        """Stop the I/O loop.

        If the event loop is not currently running, the next call to `start()`
        will return immediately.

        To use asynchronous methods from otherwise-synchronous code (such as
        unit tests), you can start and stop the event loop like this::

          ioloop = IOLoop()
          async_method(ioloop=ioloop, callback=ioloop.stop)
          ioloop.start()

        ``ioloop.start()`` will return after ``async_method`` has run
        its callback, whether that callback was invoked before or
        after ``ioloop.start``.

        Note that even after `stop` has been called, the `IOLoop` is not
        completely stopped until `IOLoop.start` has also returned.
        Some work that was scheduled before the call to `stop` may still
        be run before the `IOLoop` shuts down.
        """
        raise NotImplementedError()

    def run_sync(self, func, timeout=None):
        """Starts the `IOLoop`, runs the given function, and stops the loop.

        The function must return either a yieldable object or
        ``None``. If the function returns a yieldable object, the
        `IOLoop` will run until the yieldable is resolved (and
        `run_sync()` will return the yieldable's result). If it raises
        an exception, the `IOLoop` will stop and the exception will be
        re-raised to the caller.

        The keyword-only argument ``timeout`` may be used to set
        a maximum duration for the function.  If the timeout expires,
        a `TimeoutError` is raised.

        This method is useful in conjunction with `tornado.gen.coroutine`
        to allow asynchronous calls in a ``main()`` function::

            @gen.coroutine
            def main():
                # do stuff...

            if __name__ == '__main__':
                IOLoop.current().run_sync(main)

        .. versionchanged:: 4.3
           Returning a non-``None``, non-yieldable value is now an error.
        """
        future_cell = [None]
        def run():
            try:
                result = func()
                if result is not None:
                    from tornados.gen import convert_yielded
                    result = convert_yielded(result)
            except Exception:
                future_cell[0] = TracebackFuture()
                future_cell[0].set_exc_info(sys.exc_info())
            else:
                if is_future(result):
                    future_cell[0] = result
                else:
                    future_cell[0] = TracebackFuture()
                    future_cell[0].set_result(result)
            self.add_future(future_cell[0], lambda future: self.stop())

        self.add_callback(run)      # 将闭包 run添加回调
        if timeout is not None:
            timeout_handle = self.add_timeout(self.time() + timeout, self.stop)

        self.start()                # 开始io loop
        # 下面是处理执行结果
        if timeout is not None:
            self.remove_timeout(timeout_handle)
        if not future_cell[0].done():
            raise TimeoutError('Operation timed out after %s seconds' % timeout)
        return future_cell[0].result()

    def time(self):
        """Returns the current time according to the `IOLoop`'s clock.

        The return value is a floating-point number relative to an
        unspecified time in the past.

        By default, the `IOLoop`'s time function is `time.time`.  However,
        it may be configured to use e.g. `time.monotonic` instead.
        Calls to `add_timeout` that pass a number instead of a
        `datetime.timedelta` should use this function to compute the
        appropriate time, so they can work no matter what time function
        is chosen.
        """
        return time.time()

    def add_timeout(self, deadline, callback, *args, **kwargs):
        """Runs the ``callback`` at the time ``deadline`` from the I/O loop.

        Returns an opaque handle that may be passed to
        `remove_timeout` to cancel.

        ``deadline`` may be a number denoting a time (on the same
        scale as `IOLoop.time`, normally `time.time`), or a
        `datetime.timedelta` object for a deadline relative to the
        current time.  Since Tornado 4.0, `call_later` is a more
        convenient alternative for the relative case since it does not
        require a timedelta object.

        Note that it is not safe to call `add_timeout` from other threads.
        Instead, you must use `add_callback` to transfer control to the
        `IOLoop`'s thread, and then call `add_timeout` from there.

        Subclasses of IOLoop must implement either `add_timeout` or
        `call_at`; the default implementations of each will call
        the other.  `call_at` is usually easier to implement, but
        subclasses that wish to maintain compatibility with Tornado
        versions prior to 4.0 must use `add_timeout` instead.

        .. versionchanged:: 4.0
           Now passes through ``*args`` and ``**kwargs`` to the callback.
        """
        if isinstance(deadline, numbers.Real):
            return self.call_at(deadline, callback, *args, **kwargs)
        elif isinstance(deadline, datetime.timedelta):
            return self.call_at(self.time() + timedelta_to_seconds(deadline),
                                callback, *args, **kwargs)
        else:
            raise TypeError("Unsupported deadline %r" % deadline)

    def call_later(self, delay, callback, *args, **kwargs):
        """Runs the ``callback`` after ``delay`` seconds have passed.

        Returns an opaque handle that may be passed to `remove_timeout`
        to cancel.  Note that unlike the `asyncio` method of the same
        name, the returned object does not have a ``cancel()`` method.

        See `add_timeout` for comments on thread-safety and subclassing.

        .. versionadded:: 4.0
        """
        return self.call_at(self.time() + delay, callback, *args, **kwargs)

    def call_at(self, when, callback, *args, **kwargs):
        """Runs the ``callback`` at the absolute time designated by ``when``.

        ``when`` must be a number using the same reference point as
        `IOLoop.time`.

        Returns an opaque handle that may be passed to `remove_timeout`
        to cancel.  Note that unlike the `asyncio` method of the same
        name, the returned object does not have a ``cancel()`` method.

        See `add_timeout` for comments on thread-safety and subclassing.

        .. versionadded:: 4.0
        """
        return self.add_timeout(when, callback, *args, **kwargs)

    def remove_timeout(self, timeout):
        """Cancels a pending timeout.

        The argument is a handle as returned by `add_timeout`.  It is
        safe to call `remove_timeout` even if the callback has already
        been run.
        """
        raise NotImplementedError()

    def add_callback(self, callback, *args, **kwargs):
        """Calls the given callback on the next I/O loop iteration.

        It is safe to call this method from any thread at any time,
        except from a signal handler.  Note that this is the **only**
        method in `IOLoop` that makes this thread-safety guarantee; all
        other interaction with the `IOLoop` must be done from that
        `IOLoop`'s thread.  `add_callback()` may be used to transfer
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
        """Schedules a callback on the ``IOLoop`` when the given
        `.Future` is finished.

        The callback is invoked with one argument, the
        `.Future`.
        """
        assert is_future(future)
        callback = stack_context.wrap(callback)
        future.add_done_callback(
            lambda future: self.add_callback(callback, future))

    def _run_callback(self, callback):
        """运行一个回调与错误处理."""
        try:
            ret = callback()
            if ret is not None:
                from tornados import gen
                # Functions that return Futures typically swallow all
                # exceptions and store them in the Future.  If a Future
                # makes it out to the IOLoop, ensure its exception (if any)
                # gets logged too.
                try:
                    ret = gen.convert_yielded(ret)      # todo
                except gen.BadYieldError:
                    # It's not unusual for add_callback to be used with
                    # methods returning a non-None and non-yieldable
                    # result, which should just be ignored.
                    pass
                else:
                    self.add_future(ret, lambda f: f.result())  # todo:搞懂add_future
        except Exception:
            self.handle_callback_exception(callback)

    def handle_callback_exception(self, callback):
        """This method is called whenever a callback run by the `IOLoop`
        throws an exception.

        By default simply logs the exception as an error.  Subclasses
        may override this method to customize reporting of exceptions.

        The exception itself is not passed explicitly, but is available
        in `sys.exc_info`.
        """
        app_log.error("Exception in callback %r", callback, exc_info=True)

    def split_fd(self, fd):
        """Returns an (fd, obj) pair from an ``fd`` parameter.

        We accept both raw file descriptors and file-like objects as
        input to `add_handler` and related methods.  When a file-like
        object is passed, we must retain the object itself so we can
        close it correctly when the `IOLoop` shuts down, but the
        poller interfaces favor file descriptors (they will accept
        file-like objects and call ``fileno()`` for you, but they
        always return the descriptor itself).

        This method is provided for use by `IOLoop` subclasses and should
        not generally be used by application code.

        .. versionadded:: 4.0
        """
        try:
            return fd.fileno(), fd
        except AttributeError:
            return fd, fd

    def close_fd(self, fd):
        """Utility method to close an ``fd``.

        If ``fd`` is a file-like object, we close it directly; otherwise
        we use `os.close`.

        This method is provided for use by `IOLoop` subclasses (in
        implementations of ``IOLoop.close(all_fds=True)`` and should
        not generally be used by application code.

        .. versionadded:: 4.0
        """
        try:
            try:
                fd.close()
            except AttributeError:
                os.close(fd)
        except OSError:
            pass


class PollIOLoop(IOLoop):
    """Base class for IOLoops built around a select-like function.

    For concrete implementations, see `tornado.platform.epoll.EPollIOLoop`
    (Linux), `tornado.platform.kqueue.KQueueIOLoop` (BSD and Mac), or
    `tornado.platform.select.SelectIOLoop` (all platforms).
    """
    def initialize(self, impl, time_func=None, **kwargs):
        super(PollIOLoop, self).initialize(**kwargs)
        self._impl = impl          # 选择异步事件 kqueue or epoll or select
        if hasattr(self._impl, 'fileno'):
            set_close_exec(self._impl.fileno())     # fork 后关闭无用文件描述符
        self.time_func = time_func or time.time     # 时间函数
        self._handlers = {}                         # handlers字典, fd:event
        self._events = {}                           # events字典,存储活跃的 fd:event
        self._callbacks = []                        # 各个fd回调函数列表
        self._callback_lock = threading.Lock()
        self._timeouts = []                         # 最小堆结构，按超时时间从小到大排列的 fd 的任务堆
                                                    # 通常这个任务都会包含一个 callback
        self._cancellations = 0                     # timeout 的计数器

        # ioloop状态
        self._running = False
        self._stopped = False
        self._closing = False
        self._thread_ident = None                   # 线程标识
        self._blocking_signal_threshold = None      # 阻塞信号阈值(秒)
        self._timeout_counter = itertools.count()

        # 创建一个管道(pipe), 当我们想唤醒空闲的I/O循环时, 我们就发送伪造数据(bogus data)
        self._waker = Waker()
        # 将管道的描述符加入多路复用的IO管理, 绑定读事件和consume()操作
        self.add_handler(self._waker.fileno(),
                         lambda fd, events: self._waker.consume(),
                         self.READ)

    def close(self, all_fds=False):
        """关闭ioloop,做一些关闭操作和清除工作"""
        with self._callback_lock:
            self._closing = True
        self.remove_handler(self._waker.fileno())
        if all_fds:
            for fd, handler in self._handlers.values():
                self.close_fd(fd)
        self._waker.close()
        self._impl.close()
        self._callbacks = None
        self._timeouts = None

    def add_handler(self, fd, handler, events):
        fd, obj = self.split_fd(fd)
        self._handlers[fd] = (obj, stack_context.wrap(handler))
        self._impl.register(fd, events | self.ERROR)

    def update_handler(self, fd, events):
        fd, obj = self.split_fd(fd)
        self._impl.modify(fd, events | self.ERROR)

    def remove_handler(self, fd):
        fd, obj = self.split_fd(fd)
        self._handlers.pop(fd, None)
        self._events.pop(fd, None)
        try:
            self._impl.unregister(fd)
        except Exception:
            gen_log.debug("Error deleting fd from IOLoop", exc_info=True)

    def set_blocking_signal_threshold(self, seconds, action):
        if not hasattr(signal, "setitimer"):
            gen_log.error("set_blocking_signal_threshold requires a signal module "
                          "with the setitimer method")
            return
        self._blocking_signal_threshold = seconds
        if seconds is not None:
            signal.signal(signal.SIGALRM,
                          action if action is not None else signal.SIG_DFL)

    def start(self):
        if self._running:
            raise RuntimeError("IOLoop is already running")
        self._setup_logging()           # 设置日志
        if self._stopped:
            self._stopped = False
            return
        old_current = getattr(IOLoop._current, "instance", None)
        IOLoop._current.instance = self             # 将当前对象作为IOLoop实例
        self._thread_ident = thread.get_ident()     # 设置线程标识
        self._running = True

        # 该函数作用:是当一个信号出现时, 设置fd(必须是non-blocking)可写(with '\0')
        # 这个库常用作唤醒select or poll
        # 一个信号可能在select/poll/等事件轮询开始前抵达
        # 在python's 信号处理中, set_wakeup_fd仅仅能作用在主线程, 否则触发ValueError异常).
        #
        old_wakeup_fd = None
        if hasattr(signal, 'set_wakeup_fd') and os.name == 'posix':
            try:
                old_wakeup_fd = signal.set_wakeup_fd(self._waker.write_fileno())
                if old_wakeup_fd != -1:
                    # 已经设置,恢复以前的值。
                    # This is a little racy,
                    # but there's no clean get_wakeup_fd and in real use the
                    # IOLoop is just started once at the beginning.
                    signal.set_wakeup_fd(old_wakeup_fd)
                    old_wakeup_fd = None
            except ValueError:
                # Non-main thread, or the previous value of wakeup_fd
                # is no longer valid.
                old_wakeup_fd = None

        try:
            # 轮询开始
            while True:
                # 线程锁处理临界资源, 一次循环处理一批fd回调函数列表
                with self._callback_lock:
                    callbacks = self._callbacks
                    self._callbacks = []

                # Add any timeouts that have come due to the callback list.
                # Do not run anything until we have determined which ones
                # are ready, so timeouts that call add_timeout cannot
                # schedule anything in this iteration.
                due_timeouts = []       # 存放超时的任务
                if self._timeouts:
                    # 获取当前时间，用来判断 _timeouts 里的任务有没有超时
                    # 循环`_timeouts`列表,`_timeouts` 是heapq 堆, heap[0]总是最小的元素
                    # 这里第一个数据永远是最接近超时或已超时的, 所以下面只检查第一个元素
                    now = self.time()
                    while self._timeouts:
                        if self._timeouts[0].callback is None:
                            # 如果任务无回调,取消超时, 将_timeouts列表中第一个元素(最小的)弹出
                            heapq.heappop(self._timeouts)
                            self._cancellations -= 1            # 超时计数器 －1
                        elif self._timeouts[0].deadline <= now:  # 判断是否超时
                            # 超时就弹出并添加到已超时列表里
                            due_timeouts.append(heapq.heappop(self._timeouts))
                        else:
                            # 因为最小堆，如果没超时就直接退出循环（ 后面的数据必定未超时 ）
                            break

                    # 当超时计数器大于 512 且大于 _timeouts 长度一半时，清零计数器，
                    # 并剔除 _timeouts 中无 callbacks 的任务
                    if (self._cancellations > 512 and
                            self._cancellations > (len(self._timeouts) >> 1)):
                        self._cancellations = 0
                        self._timeouts = [x for x in self._timeouts
                                          if x.callback is not None]
                        heapq.heapify(self._timeouts)     # _timeouts最小堆处理

                # 执行回调列表里的所有回调
                for callback in callbacks:
                    self._run_callback(callback)        # todo: point
                # 执行所有超时任务列表里的回调
                for timeout in due_timeouts:
                    if timeout.callback is not None:
                        self._run_callback(timeout.callback)
                # 闭包可能会持有大量的内存, 所以在我们进入poll wait之前允许它们被释放
                callbacks = callback = due_timeouts = timeout = None    # 释放内存

                # 设置轮询超时时间
                if self._callbacks:
                    poll_timeout = 0.0      # 如果还有任务则立马执行
                elif self._timeouts:        # 如果有超时任务, 则取最近的(最小堆)超时时间-当前时间
                    poll_timeout = self._timeouts[0].deadline - self.time()
                    poll_timeout = max(0, min(poll_timeout, _POLL_TIMEOUT))
                else:
                    poll_timeout = _POLL_TIMEOUT    # 否则是默认 3600.0 s

                if not self._running:
                    break

                # 定时器类型 `ITIMER_REAL` : 按实际时间计时，计时到达将给进程发送SIGALRM信号(闹钟信号)。
                # setitimer来设置定时器
                # 当I/O循环阻塞超过 _blocking_signal_threshold 时会发送一个 SIGALRM 信号.
                # 进入poll之前调用signal.setitimer(signal.ITIMER_REAL, 0, 0)清理定时器，直到
                # poll返回后重新设置定时器。
                if self._blocking_signal_threshold is not None:
                    signal.setitimer(signal.ITIMER_REAL, 0, 0)

                try:
                    event_pairs = self._impl.poll(poll_timeout)     # 获取返回的活跃事件队
                except Exception as e:
                    # Depending on python version and IOLoop implementation,
                    # different exception types may be thrown and there are
                    # two ways EINTR might be signaled:
                    # * e.errno == errno.EINTR
                    # * e.args is like (errno.EINTR, 'Interrupted system call')
                    # poll调用可能会导致进程进入阻塞状态（sleep），这时候进程被某个系统信号唤醒后会引发EINTR错误
                    if errno_from_exception(e) == errno.EINTR:
                        continue
                    else:
                        raise

                # 设置定时器以便在I/O循环阻塞超过预期时间时发送 SIGALRM 信号。
                if self._blocking_signal_threshold is not None:
                    signal.setitimer(signal.ITIMER_REAL,
                                     self._blocking_signal_threshold, 0)

                # Pop one fd at a time from the set of pending fds and run
                # its handler. Since that handler may perform actions on
                # other file descriptors, there may be reentrant calls to
                # this IOLoop that modify self._events
                self._events.update(event_pairs)         # 将活跃事件加入 _events
                while self._events:
                    fd, events = self._events.popitem()     # 循环弹出事件
                    try:
                        fd_obj, handler_func = self._handlers[fd]   # 处理事件
                        handler_func(fd_obj, events)
                    except (OSError, IOError) as e:
                        if errno_from_exception(e) == errno.EPIPE:
                            # Happens when the client closes the connection
                            pass
                        else:
                            self.handle_callback_exception(self._handlers.get(fd))
                    except Exception:
                        self.handle_callback_exception(self._handlers.get(fd))
                fd_obj = handler_func = None
        finally:
            # reset the stopped flag so another start/stop pair can be issued
            # I/O循环结束, 重置`_stopped`状态，清理定时器，将当前IOLoop实例从当前线程移除绑定。
            self._stopped = False
            if self._blocking_signal_threshold is not None:
                signal.setitimer(signal.ITIMER_REAL, 0, 0)      # 清空 signal alarm
            IOLoop._current.instance = old_current
            if old_wakeup_fd is not None:
                signal.set_wakeup_fd(old_wakeup_fd)

    def stop(self):
        self._running = False
        self._stopped = True
        self._waker.wake()

    def time(self):
        return self.time_func()

    def call_at(self, deadline, callback, *args, **kwargs):
        timeout = _Timeout(
            deadline,
            functools.partial(stack_context.wrap(callback), *args, **kwargs),
            self)
        heapq.heappush(self._timeouts, timeout)
        return timeout

    def remove_timeout(self, timeout):
        # Removing from a heap is complicated, so just leave the defunct
        # timeout object in the queue (see discussion in
        # http://docs.python.org/library/heapq.html).
        # If this turns out to be a problem, we could add a garbage
        # collection pass whenever there are too many dead timeouts.
        timeout.callback = None
        self._cancellations += 1

    def add_callback(self, callback, *args, **kwargs):
        if thread.get_ident() != self._thread_ident:        # 判断线程标识的一致性
            # 如果不在 IOLoop'线程上, 需要与其他线程同步,
            # 或者唤醒逻辑将引起竞态
            with self._callback_lock:
                if self._closing:
                    return

                list_empty = not self._callbacks        # fd回调函数列表是否为空
                # 将callback添加到 _callbacks列表中
                self._callbacks.append(functools.partial(
                    stack_context.wrap(callback), *args, **kwargs))     # todo
                if list_empty:
                    # 如果fd回调list为空,那么肯定non-wake, 如果有元素那ioloop肯定正在poll
                    # 就无需激活了,所以fd回调列表是否为空是激活与否的先决条件。
                    # 如果不在 IOLoop's thread, 且我们已经添加了第一个callback到_callbacks空列表中
                    # 我们需要唤醒它(可能它已经自醒了,但一个偶尔额外的唤醒也是无害的).
                    # 唤醒一个 polling IOLoop 是代价是相对昂贵, 要尽量避免唤醒
                    # 这里调用wake()方法, 给ioloop pipe 发送数据,于是乎就触发了多路复用的IO管理
                    # 对 self._waker的文件描述符的监听, 则唤醒 i/o loop
                    # 我们可写一个简单的程序, 只是add_callback调用,
                    # 然后在self._waker.wake()后添加 event_pairs = self._impl.poll(3600)
                    # 就可监听到_waker文件描述符的活跃事件
                    self._waker.wake()
        else:
            if self._closing:
                return
            # If we're on the IOLoop's thread, we don't need the lock,
            # since we don't need to wake anyone, just add the
            # callback. Blindly insert into self._callbacks. This is
            # safe even from signal handlers because the GIL makes
            # list.append atomic. One subtlety is that if the signal
            # is interrupting another thread holding the
            # _callback_lock block in IOLoop.start, we may modify
            # either the old or new version of self._callbacks, but
            # either way will work.
            self._callbacks.append(functools.partial(
                stack_context.wrap(callback), *args, **kwargs))

    def add_callback_from_signal(self, callback, *args, **kwargs):
        with stack_context.NullContext():
            self.add_callback(callback, *args, **kwargs)


class _Timeout(object):
    """An IOLoop timeout, a UNIX timestamp and a callback"""

    # Reduce memory overhead when there are lots of pending callbacks
    __slots__ = ['deadline', 'callback', 'tdeadline']

    def __init__(self, deadline, callback, io_loop):
        if not isinstance(deadline, numbers.Real):
            raise TypeError("Unsupported deadline %r" % deadline)
        self.deadline = deadline
        self.callback = callback
        self.tdeadline = (deadline, next(io_loop._timeout_counter))

    # Comparison methods to sort by deadline, with object id as a tiebreaker
    # to guarantee a consistent ordering.  The heapq module uses __le__
    # in python2.5, and __lt__ in 2.6+ (sort() and most other comparisons
    # use __lt__).
    def __lt__(self, other):
        return self.tdeadline < other.tdeadline

    def __le__(self, other):
        return self.tdeadline <= other.tdeadline


class PeriodicCallback(object):
    """Schedules the given callback to be called periodically.

    The callback is called every ``callback_time`` milliseconds.
    Note that the timeout is given in milliseconds, while most other
    time-related functions in Tornado use seconds.

    If the callback runs for longer than ``callback_time`` milliseconds,
    subsequent invocations will be skipped to get back on schedule.

    `start` must be called after the `PeriodicCallback` is created.

    .. versionchanged:: 4.1
       The ``io_loop`` argument is deprecated.
    """
    def __init__(self, callback, callback_time, io_loop=None):
        self.callback = callback
        if callback_time <= 0:
            raise ValueError("Periodic callback must have a positive callback_time")
        self.callback_time = callback_time
        self.io_loop = io_loop or IOLoop.current()
        self._running = False
        self._timeout = None

    def start(self):
        """Starts the timer."""
        self._running = True
        self._next_timeout = self.io_loop.time()
        self._schedule_next()

    def stop(self):
        """Stops the timer."""
        self._running = False
        if self._timeout is not None:
            self.io_loop.remove_timeout(self._timeout)
            self._timeout = None

    def is_running(self):
        """Return True if this `.PeriodicCallback` has been started.

        .. versionadded:: 4.1
        """
        return self._running

    def _run(self):
        if not self._running:
            return
        try:
            return self.callback()
        except Exception:
            self.io_loop.handle_callback_exception(self.callback)
        finally:
            self._schedule_next()

    def _schedule_next(self):
        if self._running:
            current_time = self.io_loop.time()

            if self._next_timeout <= current_time:
                callback_time_sec = self.callback_time / 1000.0
                self._next_timeout += (math.floor((current_time - self._next_timeout) /
                                                  callback_time_sec) + 1) * callback_time_sec

            self._timeout = self.io_loop.add_timeout(self._next_timeout, self._run)
