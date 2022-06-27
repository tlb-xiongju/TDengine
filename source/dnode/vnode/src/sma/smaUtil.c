/*
 * Copyright (c) 2019 TAOS Data, Inc. <jhtao@taosdata.com>
 *
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

#include "sma.h"

// smaFileUtil ================

#define TD_FILE_HEAD_SIZE 512

#define TD_FILE_STATE_OK  0
#define TD_FILE_STATE_BAD 1

#define TD_FILE_INIT_MAGIC 0xFFFFFFFF


static int32_t tdEncodeTFInfo(void **buf, STFInfo *pInfo);
static void   *tdDecodeTFInfo(void *buf, STFInfo *pInfo);

static int32_t tdEncodeTFInfo(void **buf, STFInfo *pInfo) {
  int32_t tlen = 0;

  tlen += taosEncodeFixedU32(buf, pInfo->magic);
  tlen += taosEncodeFixedU32(buf, pInfo->ftype);
  tlen += taosEncodeFixedU32(buf, pInfo->fver);
  tlen += taosEncodeFixedI64(buf, pInfo->fsize);

  return tlen;
}

static void *tdDecodeTFInfo(void *buf, STFInfo *pInfo) {
  buf = taosDecodeFixedU32(buf, &(pInfo->magic));
  buf = taosDecodeFixedU32(buf, &(pInfo->ftype));
  buf = taosDecodeFixedU32(buf, &(pInfo->fver));
  buf = taosDecodeFixedI64(buf, &(pInfo->fsize));
  return buf;
}

int64_t tdWriteTFile(STFile *pTFile, void *buf, int64_t nbyte) {
  ASSERT(TD_FILE_OPENED(pTFile));

  int64_t nwrite = taosWriteFile(pTFile->pFile, buf, nbyte);
  if (nwrite < nbyte) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nwrite;
}

int64_t tdSeekTFile(STFile *pTFile, int64_t offset, int whence) {
  ASSERT(TD_FILE_OPENED(pTFile));

  int64_t loffset = taosLSeekFile(TD_FILE_PFILE(pTFile), offset, whence);
  if (loffset < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return loffset;
}

int64_t tdReadTFile(STFile *pTFile, void *buf, int64_t nbyte) {
  ASSERT(TD_FILE_OPENED(pTFile));

  int64_t nread = taosReadFile(pTFile->pFile, buf, nbyte);
  if (nread < 0) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return nread;
}

int32_t tdUpdateTFileHeader(STFile *pTFile) {
  char buf[TD_FILE_HEAD_SIZE] = "\0";

  if (tdSeekTFile(pTFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  void *ptr = buf;
  tdEncodeTFInfo(&ptr, &(pTFile->info));

  taosCalcChecksumAppend(0, (uint8_t *)buf, TD_FILE_HEAD_SIZE);
  if (tdWriteTFile(pTFile, buf, TD_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  return 0;
}

int32_t tdLoadTFileHeader(STFile *pTFile, STFInfo *pInfo) {
  char     buf[TD_FILE_HEAD_SIZE] = "\0";
  uint32_t _version;

  ASSERT(TD_FILE_OPENED(pTFile));

  if (tdSeekTFile(pTFile, 0, SEEK_SET) < 0) {
    return -1;
  }

  if (tdReadTFile(pTFile, buf, TD_FILE_HEAD_SIZE) < 0) {
    return -1;
  }

  if (!taosCheckChecksumWhole((uint8_t *)buf, TD_FILE_HEAD_SIZE)) {
    terrno = TSDB_CODE_FILE_CORRUPTED;
    return -1;
  }

  void *pBuf = buf;
  pBuf = tdDecodeTFInfo(pBuf, pInfo);
  return 0;
}

void tdUpdateTFileMagic(STFile *pTFile, void *pCksm) {
  pTFile->info.magic = taosCalcChecksum(pTFile->info.magic, (uint8_t *)(pCksm), sizeof(TSCKSUM));
}

int64_t tdAppendTFile(STFile *pTFile, void *buf, int64_t nbyte, int64_t *offset) {
  ASSERT(TD_FILE_OPENED(pTFile));

  int64_t toffset;

  if ((toffset = tdSeekTFile(pTFile, 0, SEEK_END)) < 0) {
    return -1;
  }

  ASSERT(pTFile->info.fsize == toffset);

  if (offset) {
    *offset = toffset;
  }

  if (tdWriteTFile(pTFile, buf, nbyte) < 0) {
    return -1;
  }

  pTFile->info.fsize += nbyte;

  return nbyte;
}

int32_t tdOpenTFile(STFile *pTFile, int flags) {
  ASSERT(!TD_FILE_OPENED(pTFile));

  pTFile->pFile = taosOpenFile(TD_FILE_FULL_NAME(pTFile), flags);
  if (pTFile->pFile == NULL) {
    terrno = TAOS_SYSTEM_ERROR(errno);
    return -1;
  }

  return 0;
}

void tdCloseTFile(STFile *pTFile) {
  if (TD_FILE_OPENED(pTFile)) {
    taosCloseFile(&pTFile->pFile);
    TD_FILE_SET_CLOSED(pTFile);
  }
}

void tdGetVndFileName(int32_t vid, const char *dname, const char *fname, char *outputName) {
  snprintf(outputName, TSDB_FILENAME_LEN, "vnode/vnode%d/%s/%s", vid, dname, fname);
}

int32_t tdInitTFile(STFile *pTFile, STfs *pTfs, const char *fname) {
  char    fullname[TSDB_FILENAME_LEN];
  SDiskID did = {0};

  TD_FILE_SET_STATE(pTFile, TD_FILE_STATE_OK);
  TD_FILE_SET_CLOSED(pTFile);

  memset(&(pTFile->info), 0, sizeof(pTFile->info));
  pTFile->info.magic = TD_FILE_INIT_MAGIC;

  if (tfsAllocDisk(pTfs, 0, &did) < 0) {
    terrno = TSDB_CODE_NO_AVAIL_DISK;
    return -1;
  }

  tfsInitFile(pTfs, &(pTFile->f), did, fname);

  return 0;
}

int32_t tdCreateTFile(STFile *pTFile, STfs *pTfs, bool updateHeader, int8_t fType) {
  ASSERT(pTFile->info.fsize == 0 && pTFile->info.magic == TD_FILE_INIT_MAGIC);

  pTFile->pFile = taosOpenFile(TD_FILE_FULL_NAME(pTFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
  if (pTFile->pFile == NULL) {
    if (errno == ENOENT) {
      // Try to create directory recursively
      char *s = strdup(TD_FILE_REL_NAME(pTFile));
      if (tfsMkdirRecurAt(pTfs, taosDirName(s), TD_FILE_DID(pTFile)) < 0) {
        taosMemoryFreeClear(s);
        return -1;
      }
      taosMemoryFreeClear(s);

      pTFile->pFile = taosOpenFile(TD_FILE_FULL_NAME(pTFile), TD_FILE_CREATE | TD_FILE_WRITE | TD_FILE_TRUNC);
      if (pTFile->pFile == NULL) {
        terrno = TAOS_SYSTEM_ERROR(errno);
        return -1;
      }
    } else {
      terrno = TAOS_SYSTEM_ERROR(errno);
      return -1;
    }
  }

  if (!updateHeader) {
    return 0;
  }

  pTFile->info.fsize += TD_FILE_HEAD_SIZE;
  pTFile->info.fver = 0;

  if (tdUpdateTFileHeader(pTFile) < 0) {
    tdCloseTFile(pTFile);
    tdRemoveTFile(pTFile);
    return -1;
  }

  return 0;
}

int32_t tdRemoveTFile(STFile *pTFile) { return tfsRemoveFile(TD_FILE_F(pTFile)); }

// smaXXXUtil ================
// ...