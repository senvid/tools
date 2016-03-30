#!/usr/bin/env python
# -*- coding:utf-8 -*-


"""
#execute nginx log
#mysql table

#accesslog
create table accesslog(
id int(10) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
remote_addr varchar(32) NOT NULL,
time_local datetime NOT NULL,
request varchar(10) NOT NULL,
request_url varchar(120) NOT NULL DEFAULT '',
status varchar(5) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

#errlog
create table errlog(
id int(10) UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT,
err_time datetime NOT NULL,
err_type varchar(10) NOT NULL DEFAULT '',
err_desc varchar(200) NOT NULL DEFAULT '',
clientip varchar(32) NOT NULL DEFAULT '',
serverip varchar(64) NOT NULL DEFAULT '',
request varchar(10) NOT NULL DEFAULT '',
request_url varchar(120) NOT NULL DEFAULT ''
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""
import torndb
from datetime import datetime


host = '127.0.0.1:3306'
database = 'blog'
user = 'blog'
password = 'blog'
access_dir = 'access.log'
error_dir = 'error.log'


# user with
# auto manager context
class ctxdb(torndb.Connection):
    def __init__(self, **kwargs):
        super(ctxdb, self).__init__(**kwargs)

    def __enter__(self):
        pass

    # except process
    # def __exit__(self, exc_type, exe_value, exe_tb):
    def __exit__(self, *k):
        if k[0]:
            print k
        self.close()


conn = ctxdb(host=host, database=database, user=user, password=password, charset="utf8")


def access_run(log_dir):
    with open(log_dir, 'r') as f, conn:
        for l in f:
            l = l.strip()
            if len(l) == 0:
                continue
            s = l.split(' ')
            # 189.197.21.101 - - [19/Mar/2016:07:26:18 -0400] "GET / HTTP/1.1" 400 581 "-" "-"
            remote_addr = s[0]
            time_local = s[3][1:]
            # time format
            time_local = datetime.strptime(time_local,'%d/%b/%Y:%H:%M:%S')
            request = s[5][1:]
            # 184.105.138.70 - - [28/Mar/2016:23:14:25 -0400] "\x05\x01\x00" 400 600 "-" "-"
            if len(request) > 7:
                request = 'hack'
                request_url = ''
                status = '400'
            else:
                request_url = s[6].split('?')[0]
                status = s[8]
            # print remote_addr, time_local, request, request_url, status
            # 116.237.145.125 19/Mar/2016:07:36:30 GET /archive 200

            conn.insert(
                "INSERT INTO accesslog(remote_addr, time_local, request, request_url, status) "
                "VALUES(%s,%s,%s,%s,%s)",
                remote_addr, time_local, request, request_url, status)
    print '******access log done*****'


def error_run(log_dir):
    with open(log_dir, 'r') as f, conn:
        for l in f:
            l = l.strip()
            if len(l) == 0:
                continue
            s = l.split(',')

            # 2016/03/29 04:13:22
            err_time = s[0].split("[")[0].rstrip()
            err_time = datetime.strptime(err_time,'%Y/%m/%d %H:%M:%S')
            err_type = s[0].split(" ")[2][1:-1]
            err_desc = s[0].split("]")[1].lstrip()
            if len(s) == 1:
                clientip = serverip = request = request_url = ''
            else:
                clientip = s[1].split(":")[1].lstrip()
                serverip = s[2].split(":")[1].lstrip()
                request = s[4].split(" ")[2]
                request_url = s[4].split(" ")[3].split("?")[0]

            # print "%s-%s-%s-%s-%s-%s-%s" % (err_time, err_type, err_desc, clientip, serverip, request, request_url)

            conn.insert(
                "INSERT INTO errlog(err_time, err_type, err_desc, clientip, serverip,request,request_url) "
                "VALUES(%s,%s,%s,%s,%s,%s,%s)",
                err_time, err_type, err_desc, clientip, serverip, request, request_url)
    print '******error log done*******'


if __name__ == '__main__':
    access_run(access_dir)
    error_run(error_dir)
