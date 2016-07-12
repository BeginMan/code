在 Tornado 3.1这样写异步代码：

    @asynchronous
    @gen.engine
    def get(self):
        ....

Tornado 3.1 及以上版本，可以直接使用 `@gen.coroutine` 来代替 `@asynchronous`:

    @gen.coroutine
    def get(self):
        ....


`@asynchronous` 在 tornado.web 中定义，对于使用了 `@gen.coroutine` 装饰的方法不需要再使用 `@asynchronous` 进行装饰



