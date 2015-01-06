@0xc0ee86cc66ffc50d;
using Java = import "/capnp/java.capnp";
$Java.package("org.velvia");
$Java.outerClassname("CapnColumn");

struct NaMask {
  # All columns have a bitmask for representing NA values.  On = not available

  union {
    allZeroes @0 :Void;            # every value is available
    simpleBitMask @1 :List(Bool);
    allOnes @2 :Void;              # no value is available / empty
  }
}

struct SimpleColumn {
  naMask @0 :NaMask;
  vector :union {
    intVec @1 :List(Int32);
    shortVec @2 :List(Int16);
    byteVec @3 :List(Int8);
  }
}

struct Column {
  union {
    simpleColumn @0 :SimpleColumn;
    dictStrColumn @1 :Void;
  }
}