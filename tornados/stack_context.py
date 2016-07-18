# coding=utf-8
#!/usr/bin/env python
#
# Copyright 2010 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""`StackContext` allows applications to maintain threadlocal-like state
that follows execution as it moves to other execution contexts.

The motivating examples are to eliminate the need for explicit
``async_callback`` wrappers (as in `tornado.web.RequestHandler`), and to
allow some additional context to be kept for logging.

This is slightly magic, but it's an extension of the idea that an
exception handler is a kind of stack-local state and when that stack
is suspended and resumed in a new context that state needs to be
preserved.  `StackContext` shifts the burden of restoring that state
from each call site (e.g.  wrapping each `.AsyncHTTPClient` callback
in ``async_callback``) to the mechanisms that transfer control from
one context to another (e.g. `.AsyncHTTPClient` itself, `.IOLoop`,
thread pools, etc).

Example usage::

    @contextlib.contextmanager
    def die_on_error():
        try:
            yield
        except Exception:
            logging.error("exception in asynchronous operation",exc_info=True)
            sys.exit(1)

    with StackContext(die_on_error):
        # Any exception thrown here *or in callback and its descendants*
        # will cause the process to exit instead of spinning endlessly
        # in the ioloop.
        http_client.fetch(url, callback)
    ioloop.start()

Most applications shouldn't have to work with `StackContext` directly.
Here are a few rules of thumb for when it's necessary:

* If you're writing an asynchronous library that doesn't rely on a
  stack_context-aware library like `tornado.ioloop` or `tornado.iostream`
  (for example, if you're writing a thread pool), use
  `.stack_context.wrap()` before any asynchronous operations to capture the
  stack context from where the operation was started.

* If you're writing an asynchronous library that has some shared
  resources (such as a connection pool), create those shared resources
  within a ``with stack_context.NullContext():`` block.  This will prevent
  ``StackContexts`` from leaking from one request to another.

* If you want to write something like an exception handler that will
  persist across asynchronous calls, create a new `StackContext` (or
  `ExceptionStackContext`), and make your asynchronous calls in a ``with``
  block that references your `StackContext`.
"""

from __future__ import absolute_import, division, print_function, with_statement

import sys
import threading

from tornados.util import raise_exc_info


class StackContextInconsistentError(Exception):
    pass

# 一个上下文的管理容器, 这里构造了一个线程安全的变量
# 在 Tornado 运行时当中, 它就是全局量了, 所有上下文都会保存在里面.
class _State(threading.local):
    def __init__(self):
        # _state.contexts 的类型是一个 tuple , tuple 这个类型是不可变的.
        # _state.contexts 这个名字会在不同的时候指向不同的 tuple , 搞清楚"引用"与"值"的关系.
        # self.contexts[0],tuple 中包含的是普通的 StackContext 上下文，用于异步调用时恢复上下文状态
        # self.contexts[1]为 Head Context ，用于处理异步调用抛出的异常。
        self.contexts = (tuple(), None)
_state = _State()       # 当前线程的上下文状态


class StackContext(object):
    """建立一个给定的上下文作为即将被转移的StackContext对象

    注意:参数是返回一个上下文管理器的可调用对象, 而不是上下文本身::

      with my_context():

    StackContext 将函数本身而非它的结果做参数::

      with StackContext(my_context):

    通过 with StackContext(my_context):
    将 StackContext 对象加入到当前线程的上下文中（_state.contexts）。

    with StackContext() as cb: 返回的是一个 deactivation 回调，
    执行这个回调后会将该 StackContext 设置为非活动的（active=False）。
    非活动的 StackContext 不会被传递，也就是说该 StackContext 封装的上下文,
    不会在后续执行 “回调函数” 时作为 回调函数” 的上下文环境而重建
    （注：函数 _remove_deactivated 会忽略非活动的 StackContext）。
    但是这个高级特性在大多数的应用中都不需要。
    """
    def __init__(self, context_factory):
        self.context_factory = context_factory      # 上下文工厂函数
        self.contexts = []                          # 上下文函数容器
        self.active = True

    def _deactivate(self):
        self.active = False

    def enter(self):
        context = self.context_factory()
        self.contexts.append(context)       # 保存当前上下文
        context.__enter__()                 # 执行当前上下文

    def exit(self, type, value, traceback):
        context = self.contexts.pop()       # 逐个移除保持的上下文
        context.__exit__(type, value, traceback)

    # 一个 StackContext 实例的作用就是包装一个上下文对象,
    # 代码在具体执行时, 还是使用原来的 context_factory ,
    # 但是传入的这个上下文对象会被保存到全局的 _state.contexts中.
    # 把上下文暂存起来.
    # 是为了在之后的 wrap 函数当中的, 可以给出当前的, 定义时的状态信息.
    def __enter__(self):
        # 取上下文管理器中原先的上下文, 默认 ((), None)
        self.old_contexts = _state.contexts
        # self.old_contexts[0]表示所有已保存的上下文元祖, 那么 + (self,)
        # 就将当前 StackContext 加入上下文栈中, 注意(self,) 在其后, 也就是新的上下文在最后面
        # 这里就成了((上下文0, 上下文1, ...), self)
        self.new_contexts = (self.old_contexts[0] + (self,), self)
        _state.contexts = self.new_contexts         # 更新上下文管理器
        try:
            self.enter()            # 对当前上下文处理
        except:                     # 如果异常则恢复原先的上下文环境
            _state.contexts = self.old_contexts
            raise

        return self._deactivate

    # 当前上下文环境退出处理, 然后恢复原先的上下文环境
    def __exit__(self, type, value, traceback):
        try:
            self.exit(type, value, traceback)
        finally:
            final_contexts = _state.contexts
            _state.contexts = self.old_contexts
            # 上下文一致性检查
            if final_contexts is not self.new_contexts:
                raise StackContextInconsistentError(
                    'stack_context inconsistency (may be caused by yield '
                    'within a "with StackContext" block)')

            # 结束本身的引用 这样CPython的GC能快速的垃圾处理,释放内存.
            self.new_contexts = None


class ExceptionStackContext(object):
    """特殊化 StackContext 用于异常处理

    The supplied ``exception_handler`` function will be called in the
    event of an uncaught exception in this context.  The semantics are
    similar to a try/finally clause, and intended use cases are to log
    an error, close a socket, or similar cleanup actions.  The
    ``exc_info`` triple ``(type, value, traceback)`` will be passed to the
    exception_handler function.

    If the exception handler returns true, the exception will be
    consumed and will not be propagated to other exception handlers.
    """
    def __init__(self, exception_handler):
        self.exception_handler = exception_handler
        self.active = True

    def _deactivate(self):
        self.active = False

    def exit(self, type, value, traceback):
        if type is not None:
            return self.exception_handler(type, value, traceback)

    def __enter__(self):
        self.old_contexts = _state.contexts
        # 这里需要注意一下， ExceptionStackContext 自身不会作为上下文的一部分（tuple）
        # 进行传播重建，仅作为 Head StackContext ，在异步回调时负责处理
        # StackContexts 未处理的异常。
        self.new_contexts = (self.old_contexts[0], self)
        _state.contexts = self.new_contexts

        return self._deactivate

    def __exit__(self, type, value, traceback):
        try:
            if type is not None:
                return self.exception_handler(type, value, traceback)
        finally:
            final_contexts = _state.contexts
            _state.contexts = self.old_contexts

            if final_contexts is not self.new_contexts:
                raise StackContextInconsistentError(
                    'stack_context inconsistency (may be caused by yield '
                    'within a "with StackContext" block)')

            # Break up a reference to itself to allow for faster GC on CPython.
            self.new_contexts = None


class NullContext(object):
    """清空`StackContext`.
    专门用于清空上下文，适用于处理那些不希望上下文互相污染的情况；
    Useful when creating a shared resource on demand (e.g. an
    `.AsyncHTTPClient`) where the stack that caused the creating is
    not relevant to future operations.
    """
    def __enter__(self):
        self.old_contexts = _state.contexts
        _state.contexts = (tuple(), None)

    def __exit__(self, type, value, traceback):
        _state.contexts = self.old_contexts


def _remove_deactivated(contexts):
    """
    从上下文栈中移除非活动的StackContext上下文
    """
    # 从上下文栈(tuple)中移除掉非活动（不需要传播）的 StackContext 上下文。
    stack_contexts = tuple([h for h in contexts[0] if h.active])
    # 从上下文栈选择最后一个 Head Context，作为 new Head。
    # 在这里head = context[1]就是最后一个上下文self(即最后一个被添加的上下文的StackContext对象)
    head = contexts[1]
    # 遍历head, 如果存在且为非活动状态,则在上下文栈中向前(上级)取context[1]
    while head is not None and not head.active:
        head = head.old_contexts[1]

    # 处理上下文链，对于不需要传播而被设置为非活动的 StackContext 上下文节点，在 With
    # 调用结束后并没有从上下文链中移除，这段代码负责清理上下文链。
    ctx = head
    while ctx is not None:
        parent = ctx.old_contexts[1]

        while parent is not None:
            if parent.active:
                break

            # 移除非活动的 parent 上下文节点
            ctx.old_contexts = parent.old_contexts
            parent = parent.old_contexts[1]

        ctx = parent

    return (stack_contexts, head)


def wrap(fn):
    """wrap(fn) 函数是上下文调度的“核心”，它通过闭包将当前上下文保存在变量cap_contexts 中，
    并返回一个可调用的（函数）对象（wrapped 或者 null_wrapper）作为
    回调函数 fn 的 wrapper，在之后被调用时（在其他线程或者在相同线程异步调用）会从
    cap_contexts 中恢复保存的上下文，然后执行 fn 函数。
    """
    # 检查函数是否为None或已经被包装, 是则直接返回
    if fn is None or hasattr(fn, '_wrapped'):
        return fn

    # 保存包装时的上下文状态
    # 在闭包中，无法修改外部作用域的局部变量，将外部作为左值将会被认为是闭包内部的局部变量
    # 因此为了在闭包(wrapped)内部对_state.contexts进行修改，
    # 将其放入list中，这样即使list不能被修改，但list内的元素可以被修改
    # TODO: Any other better way to store contexts and update them in wrapped function?
    cap_contexts = [_state.contexts]
    # 上下文管理器栈为空(没有上下文)，则执行时无需进入上下文，这里进行空包装即可
    if not cap_contexts[0][0] and not cap_contexts[0][1]:
        def null_wrapper(*args, **kwargs):
            try:
                current_state = _state.contexts
                _state.contexts = cap_contexts[0]
                return fn(*args, **kwargs)
            finally:
                _state.contexts = current_state
        null_wrapper._wrapped = True    # 设置包装标志，防止重复包装
        return null_wrapper

    # 如果上下文管理器栈不为空，需要进入“调用时”的上下文
    def wrapped(*args, **kwargs):
        ret = None
        # 取出所有上下文管理器，移除那些已经设置为失效的StackContext，
        # 然后从头到尾对StackContext调用enter以恢复包装时的上下文

        try:
            # 捕获旧状态, 保存执行时的上下文状态，用于最后恢复
            current_state = _state.contexts

            # 删除无效的条目, 移除调用过_deactivate的StackContext
            cap_contexts[0] = contexts = _remove_deactivated(cap_contexts[0])

            # 设置为调用时的上下文状态
            _state.contexts = contexts

            # 当前异常
            exc = (None, None, None)
            top = None

            # 应用堆栈上下文
            last_ctx = 0
            stack = contexts[0]

            # 对调用时的上下文管理器栈，从头到尾调用enter来设置上下文
            for n in stack:
                try:
                    n.enter()
                    last_ctx += 1
                except:
                    # 如果在设置上下文期间抛出异常，设置top为上一个管理器，因为当前管理器没有成功进入
                    exc = sys.exc_info()
                    top = n.old_contexts[1]

            # 如果设置上下文都没抛出异常，调用该函数(此时执行时上下文已和调用时的一致)
            if top is None:
                try:
                    ret = fn(*args, **kwargs)       # 执行回调
                except:
                    exc = sys.exc_info()
                    # 如果在执行函数期间抛出异常，设置top为当前上下文管理器(栈顶)，因为当前管理器已成功进入
                    top = contexts[1]

            # 处理异常，top为应该处理异常的上下文管理器
            if top is not None:
                exc = _handle_exception(top, exc)
            else:
                # 如果没异常，反向调用exit以退出之前进入的上下文
                while last_ctx > 0:
                    last_ctx -= 1
                    c = stack[last_ctx]

                    try:
                        c.exit(*exc)
                    except:
                        # 如果在退出下文期间抛出异常，设置top为上一个管理器，因为当前管理器已经退出过了
                        exc = sys.exc_info()
                        top = c.old_contexts[1]
                        break
                else:
                    top = None

                # 处理异常
                if top is not None:
                    exc = _handle_exception(top, exc)

            # 如果异常还是没被处理，只能抛出
            if exc != (None, None, None):
                raise_exc_info(exc)
        finally:
            # 恢复执行时的上下文状态
            _state.contexts = current_state
        return ret

    # 设置包装标志，防止重复包装
    wrapped._wrapped = True
    return wrapped


def _handle_exception(tail, exc):
    """
    处理异常的过程可以看成是一个冒泡的过程
    """
    while tail is not None:
        try:
            if tail.exit(*exc):
                exc = (None, None, None)
        except:
            exc = sys.exc_info()

        tail = tail.old_contexts[1]

    return exc


def run_with_stack_context(context, func):
    """Run a coroutine ``func`` in the given `StackContext`.

    It is not safe to have a ``yield`` statement within a ``with StackContext``
    block, so it is difficult to use stack context with `.gen.coroutine`.
    This helper function runs the function in the correct context while
    keeping the ``yield`` and ``with`` statements syntactically separate.

    Example::

        @gen.coroutine
        def incorrect():
            with StackContext(ctx):
                # ERROR: this will raise StackContextInconsistentError
                yield other_coroutine()

        @gen.coroutine
        def correct():
            yield run_with_stack_context(StackContext(ctx), other_coroutine)

    .. versionadded:: 3.1
    """
    with context:
        return func()

"""
结合 StackContext 与 wrap 来看整个流程, 就是 StackContext 负责在某个地方挖个坑,
在其作用域范围内, 所有的回调函数如果 wrap 了的话, 就都会在相同的坑里执行.

如果你希望某些回调函数不受之前挖的坑的影响, 有一个临时的 NullContext 工具上下文,
它会动 _state.context , 把它清空.
这里有一点可以思考, 为什么不是去动 wrap , 而在 _state.context 上动手脚?
因为 wrap 是硬编码在 add_callback 这类函数当中的, 但是它们都受 _state.context 的影响,
所以换个思维去动 _state.context 就可以达到目的.

refs:
- https://www.zouyesheng.com/context-in-async-env.html
- http://strawhatfy.github.io/2015/07/27/tornado.stack_context/
"""

