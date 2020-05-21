import std/posix except Time
import std/streams
import std/os
import std/times
import std/strutils
import std/options
import std/hashes
import std/macros

import lmdb
import nesm

const
  ISO8601forDB* = initTimeFormat "yyyy-MM-dd\'T\'HH:mm:ss\'.\'fff"
  # we probably only need one, but
  # we might need as many as two for a migration
  MAXDBS = 2
  dbname = ".persist-db"
  packOid = when defined(release): false else: true

when not declaredInScope(oids):

  serializable:
    type
      Oid* = object
        time*: int32
        fuzz*: int32
        count*: int32

else:
  toSerializable(Oid)

serializable:
  type

    PersistentHash* = distinct int64

serializable:
  type

    PersistentObj* = object
      #phash: PersistentHash
      data*: string
      dirty*: bool
      persisted*: bool
      when packOid:
        oid*: Oid

#toSerializable(PersistentObj)

const
  invalidOid = Oid(time: 0, fuzz: 0, count: 0)


when not declaredInScope(oids):
  import std/endians

  proc `==`*(oid1: Oid, oid2: Oid): bool =
    ## Compare two Mongo Object IDs for equality
    return (oid1.time == oid2.time) and (oid1.fuzz == oid2.fuzz) and
            (oid1.count == oid2.count)

  proc hash*(oid: Oid): Hash =
    ## Generate hash of Oid for use in hashtables
    var h: Hash = 0
    h = h !& hash(oid.time)
    h = h !& hash(oid.fuzz)
    h = h !& hash(oid.count)
    result = !$h

  proc hexbyte*(hex: char): int =
    case hex
    of '0'..'9': result = (ord(hex) - ord('0'))
    of 'a'..'f': result = (ord(hex) - ord('a') + 10)
    of 'A'..'F': result = (ord(hex) - ord('A') + 10)
    else: discard

  proc parseOid*(str: cstring): Oid =
    ## parses an OID.
    var bytes = cast[cstring](addr(result.time))
    var i = 0
    while i < 12:
      bytes[i] = chr((hexbyte(str[2 * i]) shl 4) or hexbyte(str[2 * i + 1]))
      inc(i)

  proc oidToString*(oid: Oid, str: cstring) =
    const hex = "0123456789abcdef"
    # work around a compiler bug:
    var str = str
    var o = oid
    var bytes = cast[cstring](addr(o))
    var i = 0
    while i < 12:
      let b = bytes[i].ord
      str[2 * i] = hex[(b and 0xF0) shr 4]
      str[2 * i + 1] = hex[b and 0xF]
      inc(i)
    str[24] = '\0'

  proc `$`*(oid: Oid): string =
    result = newString(24)
    oidToString(oid, result)

  proc rand(): cint {.importc: "rand", header: "<stdlib.h>", nodecl.}
  proc srand(seed: cint) {.importc: "srand", header: "<stdlib.h>", nodecl.}

  var t = getTime().toUnix.int32
  srand(t)

  var
    incr: int = rand()
    fuzz: int32 = rand()

  proc genOid*(): Oid =
    ## generates a new OID.
    t = getTime().toUnix.int32
    var i = int32(atomicInc(incr))

    bigEndian32(addr result.time, addr(t))
    result.fuzz = fuzz
    bigEndian32(addr result.count, addr(i))

  proc generatedTime*(oid: Oid): Time =
    ## returns the generated timestamp of the OID.
    var tmp: int32
    var dummy = oid.time
    bigEndian32(addr(tmp), addr(dummy))
    result = fromUnix(tmp)

type
  Flag* = enum
    DryRun
    Assertions

  ModelVersion* = enum
    v0 = "(none)"
    v1 = "dragons; really alpha"

  PersistentDb* = object
    path: string
    store: FileInfo
    version: ModelVersion
    db: ptr MDBEnv
    flags: set[Flag]

template missing*(p: PersistentObj): bool = p.dirty
#template invalid*(o: Oid): bool = o == invalidOid
#template invalid*(o: Oid): bool = {o.time, o.fuzz, o.count} == {0}
template invalid*(o: Oid): bool = o.time == 0

converter toPersistentHash(h: Hash): PersistentHash =
  ## convert a Hash of undefined size to a PersistentHash of fixed size
  result = h.PersistentHash

proc newPersistentObj*(ph: PersistentHash; data: string): PersistentObj =
  #result = PersistentObj(hash: hash, data: data, dirty: true)
  #result = PersistentObj(phash: ph, dirty: true)
  result = PersistentObj(dirty: true)
  when packOid:
    result.oid = genOid()

proc newPersistentObj*(h: Hash; data: string): PersistentObj =
  result = newPersistentObj(h.toPersistentHash, data)

proc pack(gold: var PersistentObj): string =
  try:
    gold.persisted = true
    result = serialize(gold)
  finally:
    gold.persisted = false

proc unpack(input: StringStream; gold: var PersistentObj) =
  gold = deserialize(PersistentObj, input)
  gold.dirty = false

# these should only be used for assertions; not for export
proc isOpen*(self: PersistentDb): bool {.inline.} = self.db != nil
proc isClosed*(self: PersistentDb): bool {.inline.} = self.db == nil

proc close*(self: var PersistentDb) =
  ## close the database
  if self.isOpen:
    mdb_env_close(self.db)
    self.db = nil

proc removeStorage(path: string) =
  if existsDir(path):
    removeDir(path)
  assert not existsDir(path)

proc createStorage(path: string) =
  if existsDir(path):
    return
  createDir(path)
  assert existsDir(path)

proc removeDatabase*(self: var PersistentDb; flags: set[Flag]) =
  ## remove the database from the filesystem
  self.close
  assert self.isClosed
  if DryRun notin flags:
    removeStorage(self.path)

proc umaskFriendlyPerms(executable: bool): Mode =
  ## compute permissions for new files which are sensitive to umask

  # set it to 0 but read the last value
  result = umask(0)
  # set it to that value and discard zero
  discard umask(result)

  if executable:
    result = S_IWUSR.Mode or S_IRUSR.Mode or S_IXUSR.Mode or (result xor 0o777)
  else:
    result = S_IWUSR.Mode or S_IRUSR.Mode or (result xor 0o666)

proc open(self: var PersistentDb; path: string) =
  ## open the database
  assert self.isClosed
  var
    flags: cuint = 0

  if DryRun in self.flags:
    flags = MDB_RdOnly
  else:
    flags = 0
    createStorage(path)

  let mode = umaskFriendlyPerms(executable = false)

  var
    e: MDBEnv
  self.db = addr e
  if mdb_env_create(addr self.db) != 0:
    raise newException(IOError, "unable to instantiate db")
  if mdb_env_set_maxdbs(self.db, MAXDBS) != 0:
    raise newException(IOError, "unable to set max dbs")
  if mdb_env_open(self.db, path.cstring, flags, mode) != 0:
    raise newException(IOError, "unable to open db")
  assert self.isOpen

proc newTransaction(self: PersistentDb): ptr MDBTxn =
  assert self.isOpen
  var flags: cuint
  if DryRun in self.flags:
    flags = MDB_RdOnly
  else:
    flags = 0

  var
    parent: ptr MDBTxn
  if mdb_txn_begin(self.db, parent, flags = flags, addr result) != 0:
    raise newException(IOError, "unable to begin transaction")
  assert result != nil

proc newHandle(self: PersistentDb; transaction: ptr MDBTxn;
               version: ModelVersion): MDBDbi =
  assert self.isOpen
  var flags: cuint
  if DryRun in self.flags:
    flags = 0
  else:
    flags = MDB_Create
  if mdb_dbi_open(transaction, $ord(version), flags, addr result) != 0:
    raise newException(IOError, "unable to create db handle")

proc newHandle(self: PersistentDb; transaction: ptr MDBTxn): MDBDbi =
  result = self.newHandle(transaction, self.version)

when false:
  proc getModelVersion(self: PersistentDb): ModelVersion =
    assert self.isOpen
    result = ModelVersion.low
    let
      transaction = self.newTransaction
    try:
      for version in countDown(ModelVersion.high, ModelVersion.low):
        try:
          # just try to open all the known versions
          discard self.newHandle(transaction, version)
          # if we were successful, that's our version
          result = version
          break
        except Exception:
          discard
    finally:
      mdb_txn_abort(transaction)

  proc setModelVersion(self: PersistentDb; version: ModelVersion) =
    ## noop; the version is set by any write
    assert self.isOpen

when false:
  proc upgradeDatabase*(self: PersistentDb): ModelVersion =
    result = self.getModelVersion
    if result == ModelVersion.high:
      return
    var mach = newMachine[ModelVersion, ModelEvent](result)
    mach.addTransition v0, Upgrade, v1, proc () =
      self.setModelVersion(v1)

    while result != ModelVersion.high:
      mach.process Upgrade
      result = mach.getCurrentState

proc get(transaction: ptr MDBTxn; handle: MDBDbi; key: string): string =
  var
    key = MDBVal(mvSize: key.len.uint, mvData: key.cstring)
    value: MDBVal

  if mdb_get(transaction, handle, addr(key), addr(value)) != 0:
    raise newException(IOError, "unable to get value for key")

  result = newStringOfCap(value.mvSize)
  result.setLen(value.mvSize)
  copyMem(cast[pointer](result.cstring), cast[pointer](value.mvData),
          value.mvSize)
  assert result.len == value.mvSize.int

proc put(transaction: ptr MDBTxn; handle: MDBDbi;
         key: string; value: string; flags = 0) =
  var
    key = MDBVal(mvSize: key.len.uint, mvData: key.cstring)
    value = MDBVal(mvSize: value.len.uint, mvData: value.cstring)

  if mdb_put(transaction, handle, addr key, addr value, flags.cuint) != 0:
    raise newException(IOError, "unable to put value for key")

#proc `==`(a, b: PersistentHash): bool {.borrow.}
#proc `$`(h: PersistentHash): string {.borrow.}

when false:
  proc fetchViaOid(transaction: ptr MDBTxn;
                   handle: MDBDbi; oid: Oid): Option[string] =
    try:
      result = get(transaction, handle, $oid).some
    finally:
      mdb_txn_abort(transaction)

  proc fetchVia(self: PersistentDb; oid: Oid): Option[string] =
    assert self.isOpen
    let
      transaction = self.newTransaction
      handle = self.newHandle(transaction)
    result = fetchViaOid(transaction, handle, oid)

  iterator matching(self: PersistentDb; h: PersistentHash): PersistentObj =
    assert self.isOpen
    let
      transaction = self.newTransaction
    var
      stream = newStringStream()

    try:
      var
        result: PersistentObj
        handle = self.newHandle(transaction)
      assert not result.dirty
      result.dirty = true
      let
        existing = transaction.get(handle, $h) # FIXME: impl this

      # write the record and rewind the stream
      stream.write existing
      stream.setPosition(0)

      # unpack the record into the result and yield it
      unpack(stream, result)
      #if h == result.phash:
      if true:
        yield result
    finally:
      stream.close
      mdb_txn_abort(transaction)

  #template attemptRead(self: PersistentDb; key: string): untyped =

proc read*(self: PersistentDb; oid: Oid): PersistentObj =
  assert self.isOpen
  assert not result.dirty
  result.dirty = true
  var
    stream = newStringStream()
  let
    transaction = self.newTransaction

  try:
    var
      handle = self.newHandle(transaction)
    let
      existing = transaction.get(handle, $oid)

    # write the record and rewind the stream
    stream.write existing
    stream.setPosition(0)

    # unpack the record into the result
    unpack(stream, result)
  except:
    when packOid:
      result.oid = invalidOid
  finally:
    stream.close
    mdb_txn_abort(transaction)

proc write*(self: PersistentDb; oid: Oid; gold: var PersistentObj) =
  assert self.isOpen
  assert gold.dirty
  let
    transaction = self.newTransaction
    handle = self.newHandle(transaction)
  try:
    transaction.put(handle, $oid, pack(gold), MDB_NoOverWrite)
    if mdb_txn_commit(transaction) != 0:
      raise newException(IOError, "unable to commit transaction")
    gold.dirty = false
  except Exception:
    mdb_txn_abort(transaction)
    raise

proc storagePath(filename: string): string =
  ## make up a good path for the database file
  var (head, tail) = filename.normalizedPath.splitPath
  # we're gonna assume that if you are pointing to a .golden-lmdb,
  # and you named/renamed it, that you might not want the leading `.`
  if not filename.endsWith(dbname):
    tail = "." & tail & dbname
  result = head / tail

proc open*(filename: string; flags: set[Flag]): PersistentDb =
  ## instantiate a database using the filename
  result.db = nil
  result.path = storagePath(filename)
  result.flags = flags
  result.open(result.path)
  when false:
    result.setModelVersion result.upgradeDatabase()
  result.store = getFileInfo(result.path)
