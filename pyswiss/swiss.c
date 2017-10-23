#include <stddef.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <ctype.h>
#include "swiss.h"

size_t BUFFER_SIZE = 1024;

entry_a init_entries(size_t maxsize) {
    entry_a entries;
    entries.entries = malloc(maxsize * sizeof(entry_t));
    if (entries.entries == NULL) {
        fprintf(stderr, "Memory error\n");
        exit(1);
    }
    entries.maxsize = maxsize;
    entries.cursize = 0;
    return entries;
}

void delete_entries(entry_a *a) {
    unsigned int i, j;
    for (i = 0; i < a->cursize; i++) {
        for (j = 0; j < a->entries[i].n_sec; j++) {
            free(a->entries[i].sec[j]);
        }
        free(a->entries[i].sec);
    }

    free(a->entries);
}

void add_entry(entry_a *a, entry_t e) {
    if (a->cursize == a->maxsize) {
        a->maxsize *= 2;
        a->entries = realloc(a->entries, a->maxsize * sizeof(entry_t));
        if (a->entries == NULL) {
            fprintf(stderr, "Memory error\n");
            exit(1);
        }
    }

    strcpy(a->entries[a->cursize].ac, e.ac);
    strcpy(a->entries[a->cursize].crc64, e.crc64);
    strcpy(a->entries[a->cursize].name, e.name);
    a->entries[a->cursize].is_reviewed = e.is_reviewed;
    a->entries[a->cursize].is_fragment = e.is_fragment;
    a->entries[a->cursize].day = e.day;
    a->entries[a->cursize].month = e.month;
    a->entries[a->cursize].year = e.year;
    a->entries[a->cursize].tax_id = e.tax_id;
    a->entries[a->cursize].len = e.len;

    if (e.n_sec) {
        a->entries[a->cursize].sec = malloc(e.n_sec * sizeof(char *));
        unsigned int i = 0;
        for (; i < e.n_sec; i++) {
            a->entries[a->cursize].sec[i] = (char *)malloc(16);
            strcpy(a->entries[a->cursize].sec[i], e.sec[i]);
        }
    }

    a->entries[a->cursize].n_sec = e.n_sec;
    a->cursize++;
}

void rtrim(char *str) {
    size_t n;
    n = strlen(str);
    while (n > 0 && isspace((unsigned char)str[n - 1])) {
        n--;
    }
    str[n] = '\0';
}

/**
 * Read a file in the SwissProt format until "//", which marks a new entry
 * @param fp            pointer to a FILE object
 * @param buffer        array of chars where the string read is stored
 * @param e             pointer to an entry object
 * @param p             pointer to a pair object
 * @param pairs         pointer to an array of pairs
 * @return              int (0: EOF; >0 number of lines read; -1: invalid ID line; -2 invalid SQ lines)
 */
int parse_entry(FILE *fp, char *buffer, entry_t *e) {
    char *str, *token, *saveptr, *ptr;
    char delimiters[] = " ";
    char month[4];

    unsigned int i;
    unsigned int n = 0;

    // Reset entry
    memset(e->ac, 0, sizeof(e->ac));
    memset(e->crc64, 0, sizeof(e->crc64));
    e->is_fragment = 0;
    e->is_reviewed = 0;
    e->day = 0;
    e->month = 0;
    e->year = 0;
    e->tax_id = 0;
    e->len = 0;
    memset(e->name, 0, sizeof(e->name));
    if (e->n_sec) {
        for (i = 0; i < e->n_sec; i++) {
            free(e->sec[i]);
        }
        free(e->sec);
        e->n_sec = 0;
    }

    // Have to use a global variable, or to pass the size of the buffer as sizeof(buffer) would return the size of the pointer rather than the size of the buffer
    while (fgets(buffer, BUFFER_SIZE, fp)) {
        n++;
        rtrim(buffer);

        if (strncmp(buffer, "ID", 2) == 0) {
            i = 0;
            for (str = buffer; ; str = NULL) {
                token = strtok_r(str, delimiters, &saveptr);
                if (token == NULL)
                    break;
                else if (i == 1)
                    strcpy(e->name, token);
                else if (i == 2) {
                    ptr = strstr(token, "Reviewed");
                    if (ptr != NULL)
                        e->is_reviewed = 1;
                } else if (i == 3)
                    e->len = atoi(token);
                i++;
            }

            if (i != 5) {
                return -1;
            }
        } else if (strncmp(buffer, "AC", 2) == 0) {
            for (i = 0, str = buffer; ; str = NULL, i++) {
                token = strtok_r(str, delimiters, &saveptr);
                if (token == NULL)
                    break;
                else if (i) {
                    if (! strlen(e->ac))
                        strncpy(e->ac, token, strlen(token)-1);
                    else {
                        token[strlen(token)-1] = 0;  // Remove the semi-colon

                        if (e->n_sec)
                            e->sec = realloc(e->sec, (e->n_sec + 1) * sizeof(char *));
                        else {
                            e->sec = malloc(sizeof(char *));
                        }

                        // todo: test that alloc went fine
                        e->sec[e->n_sec] = (char *)malloc(16);
                        strcpy(e->sec[e->n_sec], token);
                        e->n_sec++;
                    }
                }
            }

        } else if (strncmp(buffer, "DT", 2) == 0) {
            ptr = strstr(buffer, "sequence version");
            if (ptr != NULL) {
                e->day = atoi(&buffer[5]);
                strncpy(month, &buffer[8], 3);
                month[3] = 0;

                if (strcmp(month, "JAN") == 0)
                    e->month = 1;
                else if (strcmp(month, "FEB") == 0)
                    e->month = 2;
                else if (strcmp(month, "MAR") == 0)
                    e->month = 3;
                else if (strcmp(month, "APR") == 0)
                    e->month = 4;
                else if (strcmp(month, "MAY") == 0)
                    e->month = 5;
                else if (strcmp(month, "JUN") == 0)
                    e->month = 6;
                else if (strcmp(month, "JUL") == 0)
                    e->month = 7;
                else if (strcmp(month, "AUG") == 0)
                    e->month = 8;
                else if (strcmp(month, "SEP") == 0)
                    e->month = 9;
                else if (strcmp(month, "OCT") == 0)
                    e->month = 10;
                else if (strcmp(month, "NOV") == 0)
                    e->month = 11;
                else
                    e->month = 12;

                e->year = atoi(&buffer[12]);
            }
        } else if (strncmp(buffer, "DE   Flags:", 11) == 0) {
            ptr = strstr(buffer, "Fragment");
            if (ptr != NULL)
                e->is_fragment =  1;
        } else if (!e->is_fragment && strncmp(buffer, "FT   NON_TER", 12) == 0) {
            e->is_fragment = 1;
        } else if (strncmp(buffer, "OX", 2) == 0) {
            i = 0;
            for (str = buffer; ; str = NULL) {
                token = strtok_r(str, "=", &saveptr);
                if (token == NULL)
                    break;
                else if (i)
                    e->tax_id = atoi(token);

                i++;
            }
        } else if (strncmp(buffer, "SQ", 2) == 0) {
            i = 0;
            for (str = buffer; ; str = NULL) {
                token = strtok_r(str, delimiters, &saveptr);
                if (token == NULL)
                    break;
                else if (i == 6)
                    strcpy(e->crc64, token);

                i++;
            }

            if (i != 8) {
                return -2;
            }
        } else if (strncmp(buffer, "//", 2) == 0) {
            return n;
        }
    }

    return 0;
}

unsigned int load(FILE *fp, entry_a *entries) {
    char buffer[BUFFER_SIZE];
    entry_t e;
    e.n_sec = 0;

    unsigned int n_entries = 0;
    unsigned long n_lines = 0;
    int s;
    while (1) {
        s = parse_entry(fp, buffer, &e);
        if (! s)
            break;
        else if (s == -1) {
            //fprintf(stderr, "invalid ID line in entry starting line %lu\n", n_lines);
            return 0;
        } else if (s == -2) {
            //fprintf(stderr, "invalid SQ line in entry starting line %lu\n", n_lines);
            return 0;
        } else {
            n_lines += s;
            n_entries++;
            add_entry(entries, e);

            // if (n_entries % 1000000 == 0)
            //    fprintf(stderr, "%u entries read\n", n_entries);
        }
    }

    // fprintf(stderr, "%u entries read\n", n_entries);
    return n_entries;
}

unsigned int open_load(char *filename, entry_a *entries) {
    FILE *fp = fopen(filename, "r");
    unsigned int n_entries = load(fp, entries);
    fclose(fp);
    return n_entries;
}

unsigned int stream(FILE *fp, FILE *fp_out) {
    char buffer[BUFFER_SIZE];
    entry_t e;

    unsigned int n_entries = 0;
    unsigned long n_lines = 0;
    unsigned int i;
    int status;
    while (1) {
        status = parse_entry(fp, buffer, &e);
        if (! status)
            break;
        else if (status == -1) {
            fprintf(stderr, "invalid ID line in entry starting line %lu\n", n_lines);
            break;
        } else if (status == -2) {
            fprintf(stderr, "invalid SQ line in entry starting line %lu\n", n_lines);
            break;
        } else {
            n_lines += status;
            n_entries++;

            printf("%s\t%s\t%c\t%c\t%d-%02d-%02d\t%d\t%u\t%s\n",
                    e.ac,
                    e.crc64,
                    e.is_reviewed ? 'S' : 'T',
                    e.is_fragment ? 'Y' : 'N',
                    e.year,
                    e.month,
                    e.day,
                    e.tax_id,
                    e.len,
                    e.name
                    );

            for (i = 0; i < e.n_sec; i++) {
                fprintf(fp_out, "%s\t%s\n", e.ac, e.sec[i]);
            }

            if (n_entries % 1000000 == 0)
                fprintf(stderr, "%u entries read\n", n_entries);
        }
    }

    fprintf(stderr, "%u entries read\n", n_entries);
    return n_entries;
}


unsigned int count_pairs(entry_a *entries) {
    unsigned int n_pairs = 0;
    unsigned int i = 0;
    for (; i < entries->cursize; i++) {
        n_pairs += entries->entries[i].n_sec;
    }

    return n_pairs;
}

int main(int argc, char** argv) {
    // Load
    FILE *fp = fopen(argv[1], "r");
    entry_a entries = init_entries(1000000);
    load(fp, &entries);
    fclose(fp);

    unsigned int i = 0, s;
    for (; i < entries.cursize; i++) {
        printf("%s\t%s\t%c\t%c\t%d-%02d-%02d\t%d\t%u\t%s\n",
                    entries.entries[i].ac,
                    entries.entries[i].crc64,
                    entries.entries[i].is_reviewed ? 'S' : 'T',
                    entries.entries[i].is_fragment ? 'Y' : 'N',
                    entries.entries[i].year,
                    entries.entries[i].month,
                    entries.entries[i].day,
                    entries.entries[i].tax_id,
                    entries.entries[i].len,
                    entries.entries[i].name
                    );

        for (s = 0; s < entries.entries[i].n_sec; s++) {
            fprintf(stderr, "%s\t%s\n", entries.entries[i].ac, entries.entries[i].sec[s]);
        }
    }

    delete_entries(&entries);


    // Stream
//    FILE *fp = fopen(argv[1], "r");
//    FILE *fp_out = fopen(argv[2], "w");
//    stream(fp, fp_out);
//    fclose(fp);
//    fclose(fp_out);

    return (EXIT_SUCCESS);
}

