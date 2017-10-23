import numpy as np
cimport numpy as np


__version__ = '0.1.1'


cdef extern from 'swiss.c':
    struct entry_t:
        char ac[16];
        char crc64[17];
        short int is_reviewed;
        short int is_fragment;
        short int day;
        short int month;
        short int year;
        int tax_id;
        int len;
        char name[17];
        size_t n_sec;
        char **sec;

    struct entry_a:
        entry_t *entries;
        size_t cursize;
        size_t maxsize

    entry_a init_entries(size_t maxsize);
    int open_load(char *filename, entry_a *entries);
    unsigned int count_pairs(entry_a *entries);
    void delete_entries(entry_a *e);


cpdef load(filename):
    cdef:
        entry_a c_entries = init_entries(50000000);
        unsigned int i = 0;
        unsigned int j = 0;
        unsigned int k = 0;
        unsigned int n_pairs;

    i = open_load(filename.encode(), &c_entries)

    if i:
        n_pairs = count_pairs(&c_entries)

        entries = np.empty(c_entries.cursize,
                           dtype=[
                               ('ac', 'S15'),
                               ('name', 'S16'),
                               ('dbcode', 'S1'),
                               ('isfrag', 'S1'),
                               ('crc64', 'S16'),
                               ('len', 'int32'),
                               ('year', 'int16'),
                               ('month', 'int16'),
                               ('day', 'int16'),
                               ('taxid', 'int32')
                           ])

        pairs = np.empty(n_pairs, dtype=[('ac', 'S15'), ('sec', 'S15')])

        for i in range(c_entries.cursize):
            entries[i] = (
                c_entries.entries[i].ac,
                c_entries.entries[i].name,
                b'S' if c_entries.entries[i].is_reviewed else b'T',
                b'Y' if c_entries.entries[i].is_fragment else b'N',
                c_entries.entries[i].crc64,
                c_entries.entries[i].len,
                c_entries.entries[i].year,
                c_entries.entries[i].month,
                c_entries.entries[i].day,
                c_entries.entries[i].tax_id
            )

            for j in range(c_entries.entries[i].n_sec):
                pairs[k] = (c_entries.entries[i].ac, c_entries.entries[i].sec[j])
                k += 1

        delete_entries(&c_entries)
    else:
        delete_entries(&c_entries)
        entries = None
        pairs = None

    return entries, pairs

