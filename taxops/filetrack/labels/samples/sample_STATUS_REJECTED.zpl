^XA
^FWN
^PW532
^LL203
^LH0,0

^FX ===== Outer border =====^FS
^FO5,5^GB523,193,3^FS

^FX ===== Status name (large, centered, single line) =====^FS
^CF0,40
^FO10,20^FB512,1,0,C,0^FDREJECTED^FS

^FX ===== Divider =====^FS
^FO18,84^GB497,2,2^FS

^FX ===== Code128 barcode — module width 1: STATUS: payload can run long =====^FS
^FX (e.g. "STATUS:PENDING INTAKE" = 22 chars); width 2 would overflow the =====^FS
^FX label at that length. PENDING physical scan-reliability confirmation. =====^FS
^BY1,2,60
^FO20,94^BCN,60,N,N,N^FDSTATUS:REJECTED^FS

^FX ===== Human-readable barcode value =====^FS
^CF0,18
^FO20,158^FDSTATUS:REJECTED^FS

^XZ
