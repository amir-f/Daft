from daft.daft import PythonScanOperator


class TestScanOperator(PythonScanOperator):
    def __init__(self) -> None:
        self.x = 1

    def _filter(self, predicate) -> "TestScanOperator":
        print(predicate)
        return (False, self)
