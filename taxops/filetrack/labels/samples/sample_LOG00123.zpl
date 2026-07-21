^XA
^FWN
^PW532
^LL203
^LH0,0

^FX ===== Outer border =====^FS
^FO5,5^GB523,193,3^FS

^FX ===== Identity line: LOG# (large, left) =====^FS
^CF0,30
^FO18,16^FDLOG# 00123^FS

^FX ===== Intake dates (right of LOG#, stacked) =====^FS
^CF0,20
^FO300,14^FDLOG-IN __/__/2026^FS
^FO300,40^FDEXT   __/__/2026^FS

^FX ===== Divider under identity block =====^FS
^FO18,66^GB497,2,2^FS

^FX ===== Code128 barcode (centerpiece) =====^FS
^FX BY sets module width (2) and ratio. BC height 70, no interpretation line (we print our own).^FS
^BY2,2,70
^FO70,80^BCN,70,N,N,N^FDLOG:00123^FS

^FX ===== Human-readable value under barcode =====^FS
^CF0,22
^FO70,158^FDLOG:00123^FS

^XZ

