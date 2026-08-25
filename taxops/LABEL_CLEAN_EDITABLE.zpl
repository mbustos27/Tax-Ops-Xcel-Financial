^XA
^FX LOG label. Left margin via higher ^FO x (^LH ignored on this printer).^FS
^FWN
^PW532
^LL203
^LS203
^LH0,0
^LT0
^MMT
^PQ1,0,1,Y

^CF0,30
^FO68,4^FDLOG# {LOGNUM}^FS

^CF0,20
^FO350,2^FDLOG-IN {LOG_IN_DATE}^FS
^FO350,28^FDEXT   __/__/{EXT_YEAR}^FS

^FX Extra gap under EXT before divider^FS
^FO68,62^GB444,2,2^FS

^BY2,2,70
^FO120,76^BCN,70,N,N,N^FD{BARCODE}^FS

^FX Bottom: barcode payload left, abbreviated client name right^FS
^CF0,18
^FO120,152^FD{BARCODE}^FS
^CF0,16
^FO316,154^FB194,1,0,R,0^FD{CLIENT}^FS

^XZ
