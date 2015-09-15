#!/usr/bin/env python
# -*- coding: utf-8 -*-

import binascii
import random

__author__ = 'Eric Pascual - CSTB (eric.pascual@cstb.fr)'


class Codec(object):
    def encode(self, s):
        raise NotImplemented()

    def decode(self, s):
        raise NotImplemented()


class Crypter(Codec):
    def __init__(self, key):
        self._key = key

    def encode(self, s):
        # build the compound data with the string to be encoded and its
        # encoding key
        s = bytearray(s + '\0' + self._key + '\0', 'utf-8')
        # compute the next 16 bytes multiple of the resulting length
        lg = ((len(s) - 1) / 16 + 1) * 16
        # expand the key by duplicating it to fill at the most the target length
        key = bytearray((self._key * (lg / len(self._key) + 1))[:lg], 'utf-8')
        # pad the string to be encrypted to the target length with random bytes
        rpad = bytearray((chr(random.randint(1, 255)) for _ in xrange(1, lg - len(s))))
        padded = (s + rpad)[:lg]
        # encode the result by some binary operation with the key
        buf = bytearray((b ^ k for b, k in zip(padded, key)))
        # output the final result as the hexadecimal representation of the data
        return binascii.hexlify(buf)

    def decode(self, s):
        s = binascii.unhexlify(s)
        lg = len(s)
        wkey = (self._key * (lg / len(self._key) + 1))[:lg]
        s = ''.join([chr(ord(c) ^ ord(k)) for c, k in zip(s, wkey)])
        try:
            s, k, _ = s.split('\0', 2)
            if k != self._key:
                raise ValueError()
        except ValueError:
            s = None
        return s


class Noop(Codec):
    def __init__(self, *args, **kwargs):
        # accept any signature so that we can substitute it to any codec
        pass

    def encode(self, s):
        return s

    def decode(self, s):
        return s