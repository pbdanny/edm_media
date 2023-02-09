class Test:
    def __init__(self):
        pass

    def show_hello(self):
        print("hello")


class Test2(Test):
    def __init__(self):
        super().__init__()

    def show_again(self):
        Test.show_hello(self)
        Test.show_hello(self)

def test():
    print("hello")