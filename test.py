class Car:
    name = "BMW"
    def do(self):
        self.df = "df"
        print("do")
        print(self.df)

d = Car()
d.do()
print(d.__dict__)