#! /usr/bin/env python3

import fastapi
import requests
import socket
import sqlite3
import threading
import time


db_file = 'nmh.db'


def create_database():
    print('Create database')
    try:
        con = sqlite3.connect(db_file)
        cur = con.cursor()
        cur.execute('CREATE TABLE hosts_seen(host TEXT NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY(host))')
        cur.execute('CREATE TABLE ports_seen(host TEXT NOT NULL, port INTEGER NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY(host, port))')
        cur.execute('PRAGMA journal_mode=wal')
        cur.close()
        con.commit()
        con.close()

    except sqlite3.OperationalError as e:
        pass


def poll_tcp_port(addr):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(0.5)
        s.connect(addr)

        return True

    except:
        return False

    finally:
        s.close()


def poller():
    print('Poller started')
    while True:
        con = None

        try:
            print('retrieve hosts.txt')
            r = requests.get('https://dns.lan.nurd.space/hosts.txt', verify=False)
            if r.status_code != 200:
                print(f'\tstatus: {r.status_code}')
                time.sleep(1)
                continue

            def poll_thread(host):
                try:
                    print(f'\ttest {host}')
                    now = int(time.time())

                    ports = []
                    for port in (22, 80, 443):
                        if poll_tcp_port((host, port)):
                            ports.append(port)

                    if len(ports) > 0:
                        try:
                            con = sqlite3.connect(db_file)
                            cur = con.cursor()
                            cur.execute("INSERT INTO hosts_seen(host, ts) VALUES(?, ?) ON CONFLICT(host) DO UPDATE SET ts=DATE('now')", (host, now))
                            for port in ports:
                                cur.execute("INSERT INTO ports_seen(host, port, ts) VALUES(?, ?, ?) ON CONFLICT(host, port) DO UPDATE SET ts=DATE('now')", (host, port, now))
                                print(f'\t\t{host} is listening on port {port}')

                        finally:
                            cur.close()
                            con.commit()
                            con.close()

                except Exception as e:
                    print(f'During probe: {e}')

            threads = []

            for line in r.text.split('\n'):
                host = line.rstrip('\n').split()
                if len(host) == 0:
                    continue
                host = host[0]

                while len(threads) > 32:
                    if threads[0].is_alive():
                        time.sleep(0.1)
                        continue
                    threads[0].join()
                    del threads[0]

                t = threading.Thread(target=poll_thread, args=(host,))
                t.start()
                threads.append(t)

            for t in threads:
                t.join()

        except Exception as e:
            print(f'exception: {e}, line number: {e.__traceback__.tb_lineno}')
            time.sleep(2.5)

        finally:
            if con != None:
                con.commit()
                con.close()

        print('Sleeping for next poll')
        time.sleep(59)

if __name__ == '__main__':
    create_database()

    th = threading.Thread(target=poller())
    th.start()

    from fastapi import FastAPI
    app = FastAPI()

    @app.get("/")
    async def root():
        return '<h2>Hello, world!</h2>'

    print('Running webserver')
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
