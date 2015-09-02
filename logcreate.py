from distutils.log import Log

__author__ = 'Judge'

import datetime

class Log:
    def __init__(self, logname):
        self.logname = logname

    def _timestamp(self):
        return datetime.datetime.today().strftime("%Y-%m-%d %H-%M-%S") + ': '

    def logwrite(self, stdout, *msg):
        log = open(self.logname, 'a')
        res_msg = ''
        for m in msg:
            res_msg += ' ' + str(m)
        msg = self._timestamp() + res_msg + '\n'
        log.write(msg)
        if stdout == 1:
            print(msg)
            # print(msg, end='')


if __name__ == '__main__':
    log = Log('test.log')
    log.logwrite('sdfsd',1, 323)