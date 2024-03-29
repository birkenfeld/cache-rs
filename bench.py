import socket
import time
import random
import optparse
import threading

parser = optparse.OptionParser()
parser.add_option('-c', action='store', help='cache host',
                  default='localhost')
parser.add_option('-n', action='store', type=int, help='number of keys',
                  default=10000)
parser.add_option('-s', action='store', type=int, help='number of clients',
                  default=10)
opts, args = parser.parse_args()
opts.n = opts.n // opts.s * opts.s  # even number of keys per client

benchmark, = args
rnd_key = b'%06x' % random.getrandbits(24)
all_msg = [b'ben/%s/k%06d=%s\n' % (rnd_key, i, rnd_key)
           for i in range(opts.n)]
all_msg_with_ttl = [b'+5@ben/%s/k%06d=%s\n' % (rnd_key, i, rnd_key)
                    for i in range(opts.n)]
all_set = set(s.strip() for s in all_msg)


def create_socket(tp=socket.SOCK_STREAM):
    s = socket.socket(socket.AF_INET, tp)
    s.connect((opts.c, 14869))
    return s


def connect(nmain, nsub=None):
    mains = []
    for i in range(nmain):
        mains.append(create_socket())
    subs = []
    for i in range(opts.s if nsub is None else nsub):
        sub = create_socket()
        sub.sendall(b'ben/%s/k:\n' % rnd_key)
        subs.append(sub)
    time.sleep(0.2)
    return mains, subs


def recvall(s, length=None):
    res = b''
    start = time.time()
    length = length or len(b''.join(all_msg))
    while len(res) < length and time.time() - start < 10:
        res += s.recv(length)
    return set(res.splitlines())


def udp():
    mains, subs = connect(1)
    mains[0].sendall(b''.join(all_msg))
    mains[0].sendall(b'ben/%s/k*\n' % rnd_key)
    msg_set = recvall(mains[0])
    assert msg_set == all_set

    t1 = time.time()
    s = create_socket(socket.SOCK_DGRAM)
    s.sendall(b'ben/%s/k*\n' % rnd_key)
    msg_set = recvall(s)
    assert msg_set == all_set
    return t1


def single_writer():
    mains, subs = connect(1)

    t1 = time.time()
    mains[0].sendall(b''.join(all_msg))
    for s in subs:
        msg_set = recvall(s)
        assert msg_set == all_set

    mains[0].sendall(b'ben/%s/k*\n' % rnd_key)
    msg_set = recvall(mains[0])
    assert msg_set == all_set
    return t1


def multi_writer():
    mains, subs = connect(opts.s)

    t1 = time.time()
    perclient = opts.n // opts.s
    for i, main in enumerate(mains):
        threading.Thread(target=lambda i=i, m=main: m.sendall(
            b''.join(all_msg[i*perclient:(i+1)*perclient]))).start()
    for s in subs:
        msg_set = recvall(s)
        assert msg_set == all_set
    return t1


def multi_writer_with_ttl():
    mains, subs = connect(opts.s)

    t1 = time.time()
    perclient = opts.n // opts.s
    for i, main in enumerate(mains):
        threading.Thread(target=lambda i=i, m=main: m.sendall(
            b''.join(all_msg_with_ttl[i*perclient:(i+1)*perclient]))).start()
    for s in subs:
        msg_set = recvall(s)
        assert msg_set == all_set
    return t1


def ask_only():
    mains, subs = connect(1, 0)

    mains[0].sendall(b''.join(all_msg))
    mains[0].sendall(b'ben/%s/k*\n' % rnd_key)
    msg_set = recvall(mains[0])
    assert msg_set == all_set
    t1 = time.time()
    mains[0].sendall(b'ben/%s/k*\n' % rnd_key)
    msg_set = recvall(mains[0])
    assert msg_set == all_set
    return t1


def ask_history():
    mains, _subs = connect(opts.s, 0)

    hist_msg = [b'%.1f@ben/%s/k000000=%d\n' % (i+0.1, rnd_key, i)
                for i in range(opts.n)]
    mains[0].sendall(b''.join(hist_msg))
    mains[0].sendall(b'ben/%s/k*\n' % rnd_key)
    expect = b'ben/%s/k000000=%d\n' % (rnd_key, opts.n - 1)
    msg_set = recvall(mains[0], len(expect))
    assert msg_set == set([expect.strip()])
    t1 = time.time()
    for main in mains:
        main.sendall(b'0-%f@ben/%s/k000000?\n' % (t1 + 10, rnd_key))
    for main in mains:
        msg_set = recvall(main, len(b''.join(hist_msg)))
        assert msg_set == set(m.strip() for m in hist_msg)
    return t1


fn = globals()[benchmark]
t1 = fn()
t2 = time.time()
print('%s: %d keys, %d subscribers: %.4f sec' % (benchmark, opts.n, opts.s,
                                                 t2 - t1))
