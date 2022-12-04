f = open('/home/tkien/project/1msong/data/rds.txt','r')
file='/home/tkien/project/1msong/data/rds.txt'
# option_rds = f.readlines(1)
# print(option_rds)
temp = [line[:-1] for line in f]
print(temp)