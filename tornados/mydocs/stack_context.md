`StackContext`实现了对上下文管理器的包装，构造函数接受一个参数`context_factory`，这是一个上下文管理器的工厂方法。调用`context_factory`可以获得上下文管理器，然后即可通过`__enter__()`和`__exit__()`进入和退出保存的上下文环境。

`StackContext`也实现了上下文管理器协议，目的是为了和上下文管理器的行为保持一致。

为什么要多此一举，对原有的上下文环境进行包装呢？如果只需维护一个上下文环境，确实不必如此，wrap函数直接通过`__enter__()`即可进入保存的“调用时“上下文环境。但是当“调用时”有多个嵌套的上下文环境时(比如A嵌套B，B又嵌套了C)，问题来了：如何保证wrap函数能够按顺序调用相应的`__enter__()`来进入“调用时“上下文环境呢？StackContext解决的正是这个问题。

先看线程局部变量`_state`，它的成员contexts是一个二元tuple：
	
	class _State(threading.local):
	    def __init__(self):
	        self.contexts = (tuple(), None)
	_state = _State()
	

>在StackContext的`__enter__()`中，StackContext将自身加入到该`contexts[0]`的尾端，并将`contexts[1]`设置为自身(self)。如果把`_state`理解为上下文嵌套中的一个状态，那么`contexts[0]`为当前状态下的StackContext栈，保存了从最外层上下文到当前上下文的所有StackContext；而`contexts[1]`由于保存的是最后一个StackContext，可以认为是栈顶指针。因此如果想进入到“调用时“的上下文环境，只需将“调用时“的`_state`取出，然后对`contexts[0]`从头到尾调用`enter()`即可。更为巧妙的是，为了能够建立状态之间的联系，每一个StackContext在保存当前状态`self.new_contexts`的同时，也保存了上一个状态`self.old_contexts。`

以A嵌套B，B又嵌套了C这种环境为例:

![](http://illustration-10018028.file.myqcloud.com/20160306211953.png)

在调用do_something时，状态为State C，即((A, B, C), C)。wrap()将State C保存下来，然后在“执行时“调用A、B、C的enter()即可使do_something获得和“调用时”一样的State C上下文。

