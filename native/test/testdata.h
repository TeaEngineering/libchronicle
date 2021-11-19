#define _GNU_SOURCE

#include <stdarg.h>
#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <libgen.h>

#include <archive.h>
#include <archive_entry.h>
#include <ftw.h>

static int copy_data(struct archive *ar, struct archive *aw) {
    int r;
    const void *buff;
    size_t size;
#if ARCHIVE_VERSION_NUMBER >= 3000000
    int64_t offset;
#else
    off_t offset;
#endif

    for (;;) {
        r = archive_read_data_block(ar, &buff, &size, &offset);
        if (r == ARCHIVE_EOF)
            return (ARCHIVE_OK);
        if (r != ARCHIVE_OK)
            return (r);
        r = archive_write_data_block(aw, buff, size, offset);
        if (r != ARCHIVE_OK) {
            return (r);
        }
    }
}

char* unpack_test_data(char* file, char* argv0) {

    char* appdir = strdup(argv0);
    char* test_archive;
    asprintf(&test_archive, "%s/%s", dirname(appdir), file);

    char* temp_dir;
    asprintf(&temp_dir, "%s/chronicle.test.XXXXXX", P_tmpdir);
    temp_dir = mkdtemp(temp_dir);

    printf("Unpacking test data from %s to %s\n", test_archive, temp_dir);

    // https://github.com/libarchive/libarchive/wiki/Examples
    struct archive *a;
    struct archive *ext;
    struct archive_entry *entry;
    int r;

    a = archive_read_new();
    archive_read_support_format_tar(a);
    archive_read_support_filter_bzip2(a);

    ext = archive_write_disk_new();
    archive_write_disk_set_options(ext, ARCHIVE_EXTRACT_TIME);
    archive_write_disk_set_standard_lookup(ext);

    if ((r = archive_read_open_filename(a, test_archive, 10240))) {
        fprintf(stderr, "%s\n", archive_error_string(a));
        return NULL;
    }
    char* dest_file;
    for (;;) {
        r = archive_read_next_header(a, &entry);
        if (r == ARCHIVE_EOF)
          break;
        if (r < ARCHIVE_OK)
          fprintf(stderr, "%s\n", archive_error_string(a));
        if (r < ARCHIVE_WARN)
          return NULL;

        asprintf(&dest_file, "%s/%s", temp_dir, archive_entry_pathname(entry));
        archive_entry_set_pathname(entry, dest_file);
        // printf(" writing %s\n", dest_file);

        r = archive_write_header(ext, entry);
        if (r < ARCHIVE_OK)
          fprintf(stderr, "%s\n", archive_error_string(ext));
        else if (archive_entry_size(entry) > 0) {
          r = copy_data(a, ext);
          if (r < ARCHIVE_OK)
            fprintf(stderr, "%s\n", archive_error_string(ext));
          if (r < ARCHIVE_WARN)
            exit(1);
        }
        r = archive_write_finish_entry(ext);
        if (r < ARCHIVE_OK)
          fprintf(stderr, "%s\n", archive_error_string(ext));
        if (r < ARCHIVE_WARN)
          return NULL;

        free(dest_file);
    }
    archive_read_close(a);
    archive_read_free(a);
    archive_write_close(ext);
    archive_write_free(ext);

    free(appdir);
    free(test_archive);
    return temp_dir;
}

static int rmFiles(const char *pathname, const struct stat *sbuf, int type, struct FTW *ftwb) {
    if(remove(pathname) < 0) {
        fprintf(stderr, "ERROR: remove failed at %s", pathname);
        return FTW_STOP;
    }
    return FTW_CONTINUE;
}

int delete_test_data(char* queuedir) {
    // Delete the directory and its contents by traversing the tree in reverse order, without crossing mount boundaries and symbolic links
    if (nftw(queuedir, rmFiles, 10, FTW_DEPTH|FTW_MOUNT|FTW_PHYS|FTW_ACTIONRETVAL) < 0) {
        perror("ERROR: ntfw");
        return 1;
    }
    return 0;
}


