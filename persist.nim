import std/os
import std/macros
import std/hashes
import std/streams

import nesm

include pdb

proc persistOne(section: NimNode): NimNode =
  debugEcho section.treeRepr
  result = newStmtList()
  result.add newColonExpr(ident"serializable", section)

macro persistent*(types: untyped): untyped =
  result = newStmtList()
  result.add newCall(ident"toSerializable", ident"Oid")

  for section in types.children:
    if section.kind == nnkTypeSection:
      result.add section
      #result.add persistOne(section)
      result.add newCall(ident"toSerializable", ident"Goat")

  debugEcho result.treeRepr

var
  db: PersistentDb

proc open*() =
  ## open all persistent databases
  if db.isClosed:
    db = open(getAppFilename() & ".include-typedesc-id-here😆", {})

proc store*[T](t: T): Oid =
  ## save object to its registered file
  when compiles(t.serialize):
    var
      s = newPersistentObj(t.hash, serialize(t))
    result = genOid()
    db.write(result, s)
  else:
    echo "attempt to serialize non-serializable type " & $typeof(t)

proc persisted*[T](t: T; h: Hash): bool =
  ## true if the object has been persisted
  var
    s = newPersistentObj(t.hash, serialize(t))
  let
    s = db.read(h)
  result = not s.oid.invalid and not s.missing

iterator load*[T](t: T; h: Hash): T =
  ## load objects with the given hash
  when compiles(t.serialize):
    discard
  else:
    echo "attempt to deserialize non-serializable type " & $typeof(t)

proc persisted*[T](t: T): bool =
  ## true if the object has been persisted
  let
    h = t.hash
  for p in load(t, h):
    result = p.persisted
    if result:
      break

proc load*[T](t: T; oid: Oid): T =
  ## load one object with the given hash
  when compiles(t.serialize):
    result = db.read(oid)
    if result.missing:
      assert false, "persistent object not found"
      result = t
  else:
    echo "attempt to deserialize non-serializable type " & $typeof(t)

proc load*[T](t: T): T =
  ## load one object using its hash
  block found:
    for p in t.load(t.hash):
      result = p
      break found
    assert false, "persistent object not found"
    result = t

proc close*() =
  ## close all persistent databases
  if db.isOpen:
    close(db)
