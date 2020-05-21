import std/hashes
import std/unittest

import persist
import nesm

when true:

  type
    Goat = object
      legs: int32
      teeths: int32
      name: string
      oid: Oid

  toSerializable(Goat)

else:
  persistent:
    type
      Goat = object
        legs: int32
        teeths: int32
        name: string
        oid: Oid

when false:
  persistent:
    type
      Goats = seq[Goat]

proc hash(g: Goat): Hash =
  var
    h: Hash = 0
  h = h !& hash(g.name)
  h = h !& hash(g.legs)
  h = h !& hash(g.teeths)
  result = !$h

suite "persistence":
  setup:
    let
      g = Goat(legs: 3, teeths: 2) #, name: "Tripod")
    persist.open()

  test "simple object store":
    echo store(g)
    check g.persisted

  test "simple object load":
    let
      tripod = load(g)
    check tripod.persisted

  when declaredInScope(Goats):
    test "save a seq":
      var
        gs: Goats
      gs.add g
      gs.store
      check gs.persisted

    test "load a seq":
      var
        gs: Goats
      for g in load[Goats]():
        gs.add g
      check gs.persisted

  teardown:
    persist.close()
