def interatorFunc():
    for i in range(0, 10):
        yield i


print(type(interatorFunc()))

g = interatorFunc()



print(g.__next__())


for i in interatorFunc():
    print(i)

