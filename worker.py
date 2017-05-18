import requests

jsonString = "{screen_name:'%s',count:%s}" % ("test2",1)
test = "http://selias.co.in/BigData/ScreenName?json=%s" % (jsonString)
requests.get(test)
