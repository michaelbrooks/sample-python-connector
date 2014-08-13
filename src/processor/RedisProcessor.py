__author__ = 'scott hendrickson'

import re
import redis
import multiprocessing
import multiprocessing.queues
import logging

engstoplist = ["un", "da", "se", "ap", "el", "morreu", "en", "la", "que", "ll", "don", "ve", "de", "gt", "lt", "com", "ly", "co", "re", "rt", "http","a","able","about","across","after","all","almost","also","am","among","an","and","any","are","as","at","be","because","been","but","by","can","cannot","could","dear","did","do","does","either","else","ever","every","for","from","get","got","had","has","have","he","her","hers","him","his","how","however","i","if","in","into","is","it","its","just","least","let","like","likely","may","me","might","most","must","my","neither","no","nor","not","of","off","often","on","only","or","other","our","own","rather","said","say","says","she","should","since","so","some","than","that","the","their","them","then","there","these","they","this","tis","to","too","twas","us","wants","was","we","were","what","when","where","which","while","who","whom","why","will","with","would","yet","you","your"]

spanstoplist = [ "un", "una" ,"unas" ,"unos" ,"uno" ,"sobre" ,"todo" ,"tambien" ,"tras" ,"otro" ,"algun" ,"alguno" ,"alguna" ,"algunos" ,"algunas" ,"ser" ,"es" ,"soy" ,"eres" ,"somos" ,"sois" ,"estoy" ,"esta" ,"estamos" ,"estais" ,"estan" ,"como" ,"en" ,"para" ,"atras" ,"porque" ,"porque" ,"estado" ,"estaba" ,"ante" ,"antes" ,"siendo" ,"ambos" ,"pero" ,"por" ,"poder" ,"puede" ,"puedo" ,"podemos" ,"podeis" ,"pueden" ,"fui" ,"fue" ,"fuimos" ,"fueron" ,"hacer" ,"hago" ,"hace" ,"hacemos" ,"haceis" ,"hacen" ,"cada" ,"fin" ,"incluso" ,"primero desde" ,"conseguir" ,"consigo" ,"consigue" ,"consigues" ,"conseguimos" ,"consiguen" ,"ir" ,"voy" ,"va" ,"vamos" ,"vais" ,"van" ,"vaya" ,"gueno" ,"ha" ,"tener" ,"tengo" ,"tiene" ,"tenemos" ,"teneis" ,"tienen" ,"el" ,"la" ,"lo" ,"las" ,"los" ,"su" ,"aqui" ,"mio" ,"tuyo" ,"ellos" ,"ellas" ,"nos" ,"nosotros" ,"vosotros" ,"vosotras" ,"si" ,"dentro" ,"solo" ,"solamente" ,"saber" ,"sabes" ,"sabe" ,"sabemos" ,"sabeis" ,"saben" ,"ultimo" ,"largo" ,"bastante" ,"haces" ,"muchos" ,"aquellos" ,"aquellas" ,"sus" ,"entonces" ,"tiempo" ,"verdad" ,"verdadero" ,"verdadera" ,"cierto" ,"ciertos" ,"cierta" ,"ciertas" ,"intentar" ,"intento" ,"intenta" ,"intentas" ,"intentamos" ,"intentais" ,"intentan" ,"dos" ,"bajo" ,"arriba" ,"encima" ,"usar" ,"uso" ,"usas" ,"usa" ,"usamos" ,"usais" ,"usan" ,"emplear" ,"empleo" ,"empleas" ,"emplean" ,"ampleamos" ,"empleais" ,"valor" ,"muy" ,"era" ,"eras" ,"eramos" ,"eran" ,"modo" ,"bien" ,"cual" ,"cuando" ,"donde" ,"mientras" ,"quien" ,"con" ,"entre" ,"sin" ,"trabajo" ,"trabajar" ,"trabajas" ,"trabaja" ,"trabajamos" ,"trabajais" ,"trabajan" ,"podria" ,"podrias" ,"podriamos" ,"podrian" ,"podriais","yo" ,"aquel"]

stoplist = engstoplist + spanstoplist

# looking for most common terms so set time to live in redis store in seconds
TIME_TO_LIVE = 90


class RedisProcessor(object):
    def __init__(self, _upstream, _enviroinment):
        self.environment = _enviroinment
        self.queue = _upstream
        self.logr = logging.getLogger("SaveThread")
        self._stopped = multiprocessing.Event()
        self.run_process = multiprocessing.Process(target=self._run)
        self._stopped = multiprocessing.Event()

    def run(self):
        self.run_process.start()

    def _run(self):
        self.logr.debug("CountRules started")
        rs = self.client()
        rs.flushall()
        self.run_loop(rs)

    def run_loop(self, rs):
        while not self._stopped.is_set():
            payload = self.next_message()
            if not None == payload:
                if "gnip" in payload:
                    if "matching_rules" in payload["gnip"]:
                        for mr in payload["gnip"]["matching_rules"]:
                            self.logr.debug("inc rule (%s)" % str(mr["value"]))
                            # Redis store of rule match counts
                            key = "[" + mr["value"] + "]"
                            rs.incr(key)
                            rs.incr("TotalRuleMatchCount")
                    else:
                        self.logr.debug("matching_rules missing")
                else:
                    self.logr.debug("gnip tag missing")
                if "body" in payload:
                    for t in re.split("\W+", payload["body"]):
                        tok = t.lower()
                        if tok not in stoplist and len(tok) > 2:
                            self.logr.debug("inc (%s)" % tok)
                            # Redis store of token counts
                            rs.incr(tok)
                            rs.expire(tok, TIME_TO_LIVE)
                            rs.incr("TotalTokensCount")
        print "Exiting Redis run loop"

    def client(self):
        return redis.StrictRedis(host=self.environment.redis_host, port=self.environment.redis_port)

    def stop(self):
        self._stopped.set()

    def stopped(self):
        return self._stopped.is_set() and self.queue.qsize() == 0

    def running(self):
        self.run_process.is_alive() and not self.stopped()

    def next_message(self):
        ret_val = None
        if self.queue.qsize() > 0:
            try:
                ret_val = self.queue.get(block=False)
            except multiprocessing.queues.Empty:
                self.logr.error("Queue was empty when trying to get next message")
        return ret_val