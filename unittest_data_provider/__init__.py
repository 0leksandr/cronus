from types import FunctionType


def data_provider(fn_data_provider):
    """Data provider decorator, allows another callable to provide the data for the test"""
    def test_decorator(fn):
        def repl(self):
            if type(fn_data_provider) in (list, tuple):
                run_tests(self, fn_data_provider)
            elif isinstance(fn_data_provider, FunctionType):
                run_tests(self, fn_data_provider(self))
            else:
                run_tests(self, fn_data_provider())

        def run_tests(self, iterable):
            k = 0
            for i in iterable:  # TODO: key value
                if isinstance(i, str):
                    assertion(self, k, (i, ))
                else:
                    assertion(self, k, i)
                k += 1

        def assertion(self, k: int, i):
            try:
                fn(self, *i)
            except AssertionError:
                print("Assertion error caught with data set #", k, ":", i)  # TODO: format
                raise

        return repl
    return test_decorator
