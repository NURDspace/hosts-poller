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
        cur.execute('CREATE TABLE hosts_seen(host TEXT NOT NULL, name TEXT NOT NULL, ts INTEGER NOT NULL, PRIMARY KEY(host))')
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
        t = time.time()
        s.connect(addr)
        return time.time() - t

    except:
        return None

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

            def poll_thread(host, name):
                try:
                    # print(f'\ttest {host}')
                    now = int(time.time())

                    ports = []
                    for port in (22, 80, 443):
                        latency = poll_tcp_port((host, port))
                        if not latency is None:
                            ports.append((port, latency))

                    if len(ports) > 0:
                        try:
                            con = sqlite3.connect(db_file)
                            cur = con.cursor()
                            cur.execute("INSERT INTO hosts_seen(host, name, ts) VALUES(?, ?, ?) ON CONFLICT(host) DO UPDATE SET ts=?", (host, name, now, now))
                            for port, latency in ports:
                                cur.execute("INSERT INTO ports_seen(host, port, ts, latency) VALUES(?, ?, ?, ?) ON CONFLICT(host, port) DO UPDATE SET ts=?, latency=?", (host, port, now, latency, now, latency))
                                # print(f'\t\t{host} is listening on port {port}')

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
                name = host[1]
                host = host[0]

                while len(threads) > 32:
                    if threads[0].is_alive():
                        time.sleep(0.1)
                        continue
                    threads[0].join()
                    del threads[0]

                t = threading.Thread(target=poll_thread, args=(host, name))
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
        time.sleep(39)

if __name__ == '__main__':
    create_database()

    th = threading.Thread(target=poller)
    th.start()

    from fastapi import FastAPI, Response
    app = FastAPI()

    @app.get('/')
    async def root():
        page = '''<!DOCTYPE html>
<html lang="en">
<head>
<title>NURDspace hosts</title>
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta charset="utf-8">
<link href="https://komputilo.nl/simple.css" rel="stylesheet" type="text/css">
</head>
<body>
<header><h1>NURDspace hosts</h1></header>
<article>
<section><p>Hover over elements to see more details.</p><script src="https://www.komputilo.nl/sorttable.js"></script><table class="sortable">'''

        now = time.time()

        try:
            con = sqlite3.connect(db_file)
            cur = con.cursor()
            # get a list of all unique ports ever probed
            cur.execute('SELECT DISTINCT(port) FROM ports_seen ORDER BY port')
            ports = [row[0] for row in cur]
            page += '<tr><th>host name</th><th class="sorttable_alpha">IP address</th>'
            for port in ports:
                page += f'<th>{port}</th>'
            page += '</tr>'

            port_query = 'SELECT port, ts, latency FROM ports_seen WHERE port in (' + ', '.join([str(port) for port in ports]) + ') AND host=? ORDER BY port'

            cur.execute('SELECT host, name, ts FROM hosts_seen ORDER BY name')
            for row in cur:
                page += f'<tr class="item"><td>{row[1]}</td><td>{row[0]}</td>'
                down = now - float(row[2])
                if down > 60:  # 60 is hosts.txt refresh time
                    page += f'<td colspan={len(ports)} title="down for {down:.2f} seconds">down</td>'
                else:
                    port_cur = con.cursor()
                    port_cur.execute(port_query, (row[0],))
                    port_results = dict()
                    for port in port_cur:
                        port_nr = int(port[0])
                        if now - port[1] <= 60:
                            port_results[port_nr] = (u'\u2713', '#40ff40', port[2])
                        else:
                            port_results[port_nr] = (u'\u26a0', '#ff4040', port[2])
                    port_cur.close()
                    for port in ports:
                        if port in port_results:
                            latency = port_results[port][2]
                            latency_str = f'{latency * 1000000:.0f} us' if latency < 0.001 else f'{latency * 1000:.0f} ms'
                            page += f'<td style="color: {port_results[port][1]}" title="{latency_str}">{port_results[port][0]}</td>'
                        else:
                            page += '<td>-</td>'
                page += f'</tr>'

            cur.close()
            con.close()

            page += '</table>'

        except sqlite3.OperationalError as e:
            print(f'SQL error: {e}')

        except Exception as e:
            print(f'other error: {e}')

        page += '''</section>
</article>
<p>Written by Folkert van Heusden</p>
<p>Original idea by Melan</p>
</body>
</html>
'''

        return Response(content=page)

    print('Running webserver')
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8001)
