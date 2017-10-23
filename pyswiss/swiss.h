#ifndef SWISS_H
#define SWISS_H

typedef struct entry_t {
    char ac[16];                // Accession ID
    char crc64[17];             // Sixty-four bit cyclic redundancy checksum
    short int is_reviewed;
    short int is_fragment;
    short int day;
    short int month;
    short int year;
    int tax_id;                 // Taxon ID
    int len;                    // Sequence length
    char name[17];              // Entry name
    size_t n_sec;
    char **sec;
} entry_t;

typedef struct entry_a {
    entry_t *entries;
    size_t cursize;
    size_t maxsize;
} entry_a;

entry_a init_entries(size_t maxsize);
void delete_entries(entry_a *e);
unsigned int count_pairs(entry_a *entries);

unsigned int stream(FILE *fp, FILE *fp_out);
unsigned int open_load(char *filename, entry_a *entries);
unsigned int load(FILE *fp, entry_a *entries);

#endif	// SWISS_H

