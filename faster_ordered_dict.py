from collections import deque

class FasterOrderedDict(dict):
    """
    Faster than using the standard library class collections.OrderedDict,
    because OrderedDict is pure Python. This class delegates every
    operation to dict/deque, which are both C-based.

    This handles only the operations that matter to the rest of Hydra and probably
    won't work for other use cases without modification.
    """
    def __init__(self):
        dict.__init__(self)
        self._elems = deque()

    def __delitem__(self, key):
        """
        slow but should be uncommon for our uses
        """
        dict.__delitem__(self, key)
        self._elems.remove(key)

    def __setitem__(self, key, data):
        if key in self:
            dict.__setitem__(self, key, data)
        else:
            self._elems.append(key)
            dict.__setitem__(self, key, data)

    def __repr__(self):
        elems = ', '.join(["'%s': %s" % (elem, self[elem])
                           for elem in self._elems])
        return '{%s}' % elems

    def __iter__(self):
        return iter(self._elems)

    def iteritems(self):
        for elem in self._elems:
            yield elem, self[elem]

    def iterkeys(self):
        return iter(self._elems)

    def itervalues(self):
        for elem in self._elems:
            yield self[elem]

    def keys(self):
        return list(self._elems)

    def values(self):
        return [self[elem] for elem in self._elems]

    def items(self):
        return [(elem, self[elem]) for elem in self._elems]