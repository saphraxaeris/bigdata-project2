import requests

#requests.post("http://selias.co.in/BigData/PrepareKeyWords", data={"val":True})
test = u'д'
print isinstance(test.decode("utf-8"), str)
print isinstance(test.decode("utf-8"), unicode)

print len(test.decode("utf-8"))

# jsonString = "{word:'%s',count:%s}" % ("test",1)
# test = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
# print test
# requests.get(test)

# jsonString = "{word:'%s',count:%s}" % ("test2",2)
# test = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
# requests.get(test)

# jsonString = "{word:'%s',count:%s}" % ("test3",3)
# test = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
# requests.get(test)

# jsonString = "{word:'%s',count:%s}" % ("test4",4)
# test = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
# requests.get(test)

# jsonString = "{word:'%s',count:%s}" % ("test5",5)
# test = "http://selias.co.in/BigData/Keyword?json=%s" % (jsonString)
# requests.get(test)