# W1 — Log-number integrity triage

_Generated: 2026-08-11T22:09:34Z · layout=`link_5` · L0-eligible=815 · bare_max=1316_

Findings-only. **No TaxOps writes.** Fixes are office edits in Drake Invoice Number and/or Tax Log.

## Headline

| Bucket | Count |
|---|---:|
| Drake collision `b_genuine_reuse` | 16 |
| Drake collision `c_family` | 2 |
| Drake collision `c_family_plus_other` | 1 |
| Drake malformed (all) | 11 |
| Tax Log repeated bare logs | 206 |
| Tax Log repeat `family_same_surname` | 2 keys (4 rows) |
| Tax Log repeat `genuine_reuse` | 204 keys (412 rows) |

### Class legend (Drake collisions)

- **a_format_variant** — same bare from different raw Invoice spellings (e.g. `250141` vs `25141`). Fix pad in Drake.
- **b_genuine_reuse** — different taxpayers, same bare, one raw form. Assign a new log to the wrong claimant in Log + Drake.
- **c_family** — claimants share a surname (household / spouse dual Drake records). Decide office rule before re-keying.
- **c_family_plus_other** — family cluster plus an unrelated third claimant.

## Drake collisions (19) — office queue

| Bare | Class | Drake claimants | Raw invoices | Tax Log (n / names) | TaxOps (n / names) | Fix |
|---|---|---|---|---|---|---|
| `141` | `b_genuine_reuse` | GOYTIA, SANDRA; PEREZ, ANDRES; VILLAREAL, RICHARD | `250141`, `25141` | 2 / PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU; CHAN, THY | 1 / VERDUZCO BALTAZAR, NESTOR | Genuine reuse of log 141: issue a new log to every Drake claimant except the keeper. TaxOps return 150 is `VERDUZCO BALTAZAR, NESTOR` — **name does not overlap** Drake/Log claimants; do not treat TaxOps as keeper — reconcile all three systems. Also normalize raw Invoice spellings ['250141', '25141']. |
| `203` | `b_genuine_reuse` | LAZO, BREANNA; NUNO, MARIBEL | `250203` | 1 / COLLAZO, MIGUEL & MAYRA | 1 / NUNO, MARIBEL M | Genuine reuse of log 203: issue a new log to every Drake claimant except the keeper. TaxOps already holds `NUNO, MARIBEL M` (return 786) — prefer that claim if it matches Log; re-key the others. |
| `206` | `b_genuine_reuse` | BRICENO, JUAN; DELGADO, TIFFANY | `250206` | 1 / SERRATO, ROCHELLE M | 1 / DELGADO, TIFFANY | Genuine reuse of log 206: issue a new log to every Drake claimant except the keeper. TaxOps already holds `DELGADO, TIFFANY` (return 2301) — prefer that claim if it matches Log; re-key the others. |
| `207` | `b_genuine_reuse` | COLLAZO, MAYRA; RAMOS, VALERIE | `250207` | 1 / GONZALEZ, MARTHA | 1 / COLLAZO, MIGUEL & MAYRA | Genuine reuse of log 207: issue a new log to every Drake claimant except the keeper. TaxOps already holds `COLLAZO, MIGUEL & MAYRA` (return 209) — prefer that claim if it matches Log; re-key the others. |
| `247` | `b_genuine_reuse` | BRAVO, VICTORIA; PERFECT SMILE MANAGEMENT CORP | `250247` | 1 / VILLALTA, ROSALINDA V | 1 / BRAVO, VICTORIA | Genuine reuse of log 247: issue a new log to every Drake claimant except the keeper. TaxOps already holds `BRAVO, VICTORIA` (return 143) — prefer that claim if it matches Log; re-key the others. |
| `335` | `b_genuine_reuse` | GALLEGOS, ALEJANDRO; ROJAS, LUIS | `250335` | 1 / ROSAS, RICARDO & MARTHA | 1 / GALLEGOS, ALEJANDRO | Genuine reuse of log 335: issue a new log to every Drake claimant except the keeper. TaxOps already holds `GALLEGOS, ALEJANDRO` (return 2327) — prefer that claim if it matches Log; re-key the others. |
| `339` | `b_genuine_reuse` | AGUILAR, AMBER; LOPEZ, SAUL | `250339` | 1 / GOMEZ, VIVIANA | 1 / LOPEZ, SAUL & YESENIA | Genuine reuse of log 339: issue a new log to every Drake claimant except the keeper. TaxOps already holds `LOPEZ, SAUL & YESENIA` (return 612) — prefer that claim if it matches Log; re-key the others. |
| `353` | `b_genuine_reuse` | AVILA, LUCY; SALAMA, ENGIE | `250353` | 1 / GONZALEZ, MARQUEZ | 1 / AVILA, LUCY & EDUARDO | Genuine reuse of log 353: issue a new log to every Drake claimant except the keeper. TaxOps already holds `AVILA, LUCY & EDUARDO` (return 351) — prefer that claim if it matches Log; re-key the others. |
| `634` | `b_genuine_reuse` | DAKAK, RAMI; HERREJON, NICHOLAS | `250634` | 2 / AN ADVISORS; SILVA, VINCENT & IRMA | 1 / DAKAK, RAMI | Genuine reuse of log 634: issue a new log to every Drake claimant except the keeper. TaxOps already holds `DAKAK, RAMI` (return 262) — prefer that claim if it matches Log; re-key the others. |
| `787` | `b_genuine_reuse` | CREATE YOUR HEALTH LLC; VICENTE, JOSE | `250787` | 1 / JACKSON, IRENE | 1 / VICENTE, JOSE | Genuine reuse of log 787: issue a new log to every Drake claimant except the keeper. TaxOps already holds `VICENTE, JOSE` (return 2422) — prefer that claim if it matches Log; re-key the others. |
| `862` | `b_genuine_reuse` | LUNA, JACINTO; MURILLO, JULIO | `250862` | 1 / YANEZ, ELEUTERIO | 1 / MURILLO, JULIO & LORETTA | Genuine reuse of log 862: issue a new log to every Drake claimant except the keeper. TaxOps already holds `MURILLO, JULIO & LORETTA` (return 852) — prefer that claim if it matches Log; re-key the others. |
| `888` | `b_genuine_reuse` | DURAN, AGUSTIN; HUERTA, JESUS | `250888` | 1 / VALENCIA, NICOLE | 1 / DURAN, AGUSTIN | Genuine reuse of log 888: issue a new log to every Drake claimant except the keeper. TaxOps already holds `DURAN, AGUSTIN` (return 2475) — prefer that claim if it matches Log; re-key the others. |
| `910` | `b_genuine_reuse` | GALLARDO, LORENZO; YANEZ, ELEUTERIO | `250910` | 1 / GAMBOA, ALEX | 1 / GALLARDO, LORENZO & MARTHA | Genuine reuse of log 910: issue a new log to every Drake claimant except the keeper. TaxOps already holds `GALLARDO, LORENZO & MARTHA` (return 365) — prefer that claim if it matches Log; re-key the others. |
| `1038` | `b_genuine_reuse` | JORGE BARAJAS, ISRAEL; MONTIEL FREYRE, SAMANTA | `251038` | 0 / — | 1 / JORGE BARAJAS, ISRAEL | Genuine reuse of log 1038: issue a new log to every Drake claimant except the keeper. TaxOps already holds `JORGE BARAJAS, ISRAEL` (return 2463) — prefer that claim if it matches Log; re-key the others. |
| `1075` | `b_genuine_reuse` | LEDESMA, DAVID; RODRIGUEZ, LEONOR | `251075` | 0 / — | 1 / LEDESMA, DAVID D & ALICIA D | Genuine reuse of log 1075: issue a new log to every Drake claimant except the keeper. TaxOps already holds `LEDESMA, DAVID D & ALICIA D` (return 580) — prefer that claim if it matches Log; re-key the others. |
| `1103` | `b_genuine_reuse` | QUINTANA, ROGELIO; SANCHEZ ARCE, EUNICE | `251103` | 1 / QUINTANA, ROGELIO JR | 1 / QUINTANA JR, ROGELIO & KAYLA M QUINTANA | Genuine reuse of log 1103: issue a new log to every Drake claimant except the keeper. TaxOps already holds `QUINTANA JR, ROGELIO & KAYLA M QUINTANA` (return 878) — prefer that claim if it matches Log; re-key the others. |
| `426` | `c_family` | HERNANDEZ, AMADO; HERNANDEZ, LYDIA | `250426` | 0 / — | 1 / HERNANDEZ, LYDIA | Household under log 426: pick one Drake primary (or issue spouse a new log). Do not merge TaxOps clients until Wave 2A trail exists. |
| `589` | `c_family` | SANTIAGO, DENISE; SANTIAGO, EUNICE | `250589` | 2 / DENISE, SANTIAGO; KIKB ENTERPRISES | 1 / DENISE, SANTIAGO | Household under log 589: pick one Drake primary (or issue spouse a new log). Do not merge TaxOps clients until Wave 2A trail exists. |
| `1095` | `c_family_plus_other` | GUTIERREZ, JOAO; GUTIERREZ, MERCEDES; RODRIGUEZ, JAVIER | `251095` | 0 / — | 1 / GUTIERREZ, MERCEDES | Family share + outsider on 1095: peel the unrelated claimant to a new log; then apply household rule to the family remainder. |

### Priority: bare `141` (I7 blocker)

- Class: `b_genuine_reuse` flags=['FORMAT_VARIANT']
- Drake: GOYTIA, SANDRA @['250141'], PEREZ, ANDRES @['25141'], VILLAREAL, RICHARD @['250141']
- Tax Log: {'n_rows': 2, 'names': ['PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU', 'CHAN, THY'], 'classification': {'kind': 'genuine_reuse', 'n_rows': 2, 'n_distinct_people': 2, 'n_surnames': 2, 'shared_surnames': [], 'names': ['CHAN, THY', 'PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU']}}
- TaxOps: return_ids=[150] names=['VERDUZCO BALTAZAR, NESTOR']
- Fix: Genuine reuse of log 141: issue a new log to every Drake claimant except the keeper. TaxOps return 150 is `VERDUZCO BALTAZAR, NESTOR` — **name does not overlap** Drake/Log claimants; do not treat TaxOps as keeper — reconcile all three systems. Also normalize raw Invoice spellings ['250141', '25141'].
- After Drake/Log fix: re-export TAXPAYER.csv → re-run A0–A4; expect I7 PASS if 141 is unique.

## Malformed invoices

| Invoice | Bare | Bucket | Name | Tax Log | Fix |
|---|---|---|---|---|---|
| `25` | `` | `truncated_season_only` | GR INDUSTRIES INC | — | Replace truncated Invoice (25 / 250) with the real season-padded log in Drake. |
| `250` | `` | `truncated_season_only` | LA MESA AUTO SALES INC | — | Replace truncated Invoice (25 / 250) with the real season-padded log in Drake. |
| `250` | `` | `truncated_season_only` | GARCIA, GILBERT | — | Replace truncated Invoice (25 / 250) with the real season-padded log in Drake. |
| `210273` | `210273` | `prior_or_non_season` | CHEVEZ, LEONEL | — | Confirm whether this is a prior-season invoice left on the TY2025 return; clear or replace with the TY2025 log, or move the return to the correct season. |
| `251355` | `1355` | `out_of_range_bare` | HARRIS JR, EDDIE | — | Bare 1355 exceeds observed ceiling 1316. Verify digits (typo / extra digit) against Tax Log assignment. |
| `50688` | `50688` | `prior_or_non_season` | ARMENTA, LUISA | — | Confirm whether this is a prior-season invoice left on the TY2025 return; clear or replace with the TY2025 log, or move the return to the correct season. |
| `23229` | `23229` | `prior_or_non_season` | AYALA, VIRGINIA | — | Confirm whether this is a prior-season invoice left on the TY2025 return; clear or replace with the TY2025 log, or move the return to the correct season. |
| `250` | `` | `truncated_season_only` | ARRUE, SILVIA | — | Replace truncated Invoice (25 / 250) with the real season-padded log in Drake. |
| `251685` | `1685` | `out_of_range_bare` | MORALES, JOSE | — | Bare 1685 exceeds observed ceiling 1316. Verify digits (typo / extra digit) against Tax Log assignment. |
| `210729` | `210729` | `prior_or_non_season` | MARQUEZ, RICARDO | — | Confirm whether this is a prior-season invoice left on the TY2025 return; clear or replace with the TY2025 log, or move the return to the correct season. |
| `2507598` | `7598` | `out_of_range_bare` | RAMIREZ, NATALIE | — | Bare 7598 exceeds observed ceiling 1316. Verify digits (typo / extra digit) against Tax Log assignment. |

## Tax Log internal repeats

XCEL 2025: **1251** non-empty log cells → **1041** distinct bare logs → **206** bare logs with >1 named row (~210 extra rows on repeated keys).

| Kind | Distinct bare logs | Total rows on those keys | Meaning |
|---|---:|---:|---|
| `family_same_surname` | 2 | 4 | Multiple first names, one surname — household under one log. Office rule. |
| `genuine_reuse` | 204 | 412 | Distinct surnames under one log — must split before any Log write-back. |

### Genuine reuse in Tax Log (office must split)

**204** bare logs. Showing all:

| Bare | n_rows | Names |
|---|---:|---|
| `4` | 3 | MACKAY, JEANNE; ORMA SERVICES, INC; PEREZ, NORA L |
| `6` | 3 | ALDAMA, MAYRA; ORMA SERVICES, INC; SOTELO, MICHELLE |
| `8` | 3 | ALVARADO, ERNESTO G & REBECCA; ORMA SERVICES  INC; PARTIDA, DYMENIQUE |
| `307` | 3 | AJRAB, YOUSEF & NATALIE KDEISS; CALISTRO, FRANK R; RAMOS, ANTONIO |
| `1` | 2 | AZUSA HOMES LLC; CEST LA VIE APPAERL INC |
| `2` | 2 | BRAVO, BYRON M; LIONS FUMIGATION INC |
| `3` | 2 | BOCANEGRA GALLEGOS, URIEL & ADRIANA; CHEANG, SARAH |
| `141` | 2 | CHAN, THY; PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU |
| `142` | 2 | AGUILAR & GARCIA ENTERPRISES; L.A. AUTO SERVICE LLC |
| `143` | 2 | LA MESA AUTO SALES INC.; VILLAREAL, RICHARD P |
| `144` | 2 | RECENDEZ, FRANCISCO D & DAISY CASAS; SALAS, SERGIO D |
| `145` | 2 | BEDARD, PETER A; GALICIA MELCHOR, GISELA |
| `146` | 2 | BURGOIN, LOUIE & FRANCES A; SANDOVAL AUTO SERVICE & TOW |
| `147` | 2 | SANDOVAL, RUDY & CLAIRE; SANTANA, HERAS |
| `148` | 2 | LOS BOMBEROS DE LA COUNTY; TAREEN, ZULQARNIAN & FOUZIA |
| `149` | 2 | DORIA, RUDY J DORIA; LOPEZ, JUAN |
| `150` | 2 | SANDOVAL, GERARDO V; VIDAL, LUIS M |
| `151` | 2 | GARCIA, FRANCISCO; MULTICARD SYSTEMS |
| `152` | 2 | CARRANZA, MAURICIO A; PACIFIC TRUCK |
| `181` | 2 | ABU TAHA, QAIS AHMAD YOUNIS; VELASQUEZ, CARLOS & MARIA E |
| `182` | 2 | CORNWELL, JOHN; GUERRERO, VICTOR M |
| `183` | 2 | CORNWELL, JOHN; JUAREZ, EDWVIGES |
| `184` | 2 | CORNWELL, JOHN; JUAREZ, EDWVIGES |
| `185` | 2 | CORNWELL, JOHN; MACIAS, DENNIS N |
| `186` | 2 | CAZARES, MARIA E; TRIGEROUS, NOELLE M |
| `5` | 2 | CASILLAS, MARIA DEL CARMEN; PADILLA, CRISTIAN F |
| `7` | 2 | CASILLAS, MARIA DEL CARMEN; GARCIA PARTIDA, JACQUELINE |
| `9` | 2 | CATALPA DEVELOPMENT LLC; LECHUGA, REBECCA |
| `271` | 2 | GONZALEZ, SHAWN M; VARGAS, CELESTINO L |
| `272` | 2 | GONZALEZ, AURELIO R; ROMERO, GRISELDA |
| `273` | 2 | CHEVEZ, LEONEL & MARY; SALAZAR QUIROZ, ROSA M |
| `274` | 2 | PEREZ & GONZALEZ, MICHAEL J & CRYSTAL; YOUSEF, QASEM |
| `275` | 2 | FLORES, CIPRIANO & MARIA; THE PRINCESS MATTRESS INC |
| `276` | 2 | AGUIRRE JIMENEZ, AMALIA; HUERTA, JUAN & ALICIA |
| `277` | 2 | CRUZ, GEORGE L; SERRANOS TIRES & AUTO REPAIRS CORP |
| `278` | 2 | MONARREZ & CERVANTES, BEATRIZ & JUAN; SERRANO LOPEZ, JESUS & JOVITA |
| `279` | 2 | RAMIREZ, ELIZABETH; SERRANO, JORGE |
| `280` | 2 | BARRIOS, SELVIN; RODRIGUEZ, AMADO |
| `281` | 2 | BARRIOS OROZCO, ISABEL A; MELENDREZ, JOSE & KARLA |
| `282` | 2 | LEDESMA, BLANCA & ADRIAN; SANCHEZ, JORGE |
| `283` | 2 | ARRUE CORLETO, NELSON A; ORTEGA, MARTHA |
| `284` | 2 | MYM ORGANICS LLC; TASHAYOD, TARA |
| `285` | 2 | RESENDEZ, FRANCISCO & MARICRUZ; RIOS, ELIZABETH |
| `286` | 2 | FRANK'S REFINISHING INC; TRUJILLO, FRANCISCO |
| `287` | 2 | AZAMAR, MISAEL; CASTILLO, JESUS & MARTHA |
| `288` | 2 | CAMERO, EDNA; CARDONA, HECTOR |
| `289` | 2 | ACOSTA, BRYANA; OROZCO SOTO, JULIO |
| `290` | 2 | RECENDEZ, GILBERTO; WRIGHT, CAGE |
| `291` | 2 | DIAZ, DANIEL; LIMA, ERASMO |
| `292` | 2 | MELENDEZ CORVERA, ELVIS M; MORENO, WILLIAM & MARIA |
| `294` | 2 | MARTINEZ, ELENA; MEDINA, YESENIA |
| `295` | 2 | MORENO GONZALEZ & LUNA, JESSICA; TORRES DE REYES, EVA |
| `296` | 2 | MARTINEZ, MEGAN G; MORALES, WILLIAM J |
| `297` | 2 | AGUAYO, MARGARITA P; ALEMAN, JOSE M |
| `298` | 2 | MAYA CASTILLO, FELIX & ALICIA ALEMAN; VARELA PARRA, FRANCISCO |
| `299` | 2 | ABU TAHA, ZAID AHMED Y & GIULIANA; YANEZ, AMBER |
| `300` | 2 | GARZA, RUMALDO; SANDOVAL, RAMON & ALMA |
| `301` | 2 | URIAS, MARTHA E; VELEZ, SALVADOR & ZONIA |
| `302` | 2 | ESTRADA, FRANCISCO & LIGIA; SOTO LOPEZ, MARIBEL |
| `308` | 2 | CONCHAS, JOSE B & MARIA D J MOJARRO; MC GILL, ROSEMARIE |
| `310` | 2 | CASTANEDA, OSIEL & ROCIO; GALINDO SOTO, PATRICIA |
| `311` | 2 | MORENO, DAVID; PANDURO, SONIA A |
| `312` | 2 | RIVAS, LUIS H; SOSA, CRISTINA |
| `313` | 2 | CRUZ, LAURA V; VELASCO, DELIA |
| `314` | 2 | RODAS, GUSTAVO & CARIDAD; SOTELO, BRENDA |
| `315` | 2 | BRAVO, VINCENT II; MIJARES, REYMUNDO & DEBORAH L |
| `316` | 2 | AGUILAR, HORACIO & ROBERTA G; ESPI, CARMEN A |
| `317` | 2 | ACOSTA, VERONICA & RUBEN; SALAZAR NUNEZ, JESUS M & CRIZTAL N |
| `318` | 2 | ARROYO & HERNANDEZ, HUMBERTO & MARIA; HENDERSON, DIANA |
| `319` | 2 | RODAS, RENAN; VASQUEZ & DURAN, ROBERT K & PALOMA |
| `320` | 2 | DAVILA, VANESSA; G'S DELIVERY SERVICES |
| `327` | 2 | JORGE DE LA OSA DENTAL CORP; TRILLOS PEINADO, YERSINO |
| `328` | 2 | IMAS INDUSTRIAL MACHINE AUTOMATION INC; OROZCO, CYNTHIA |
| `329` | 2 | MC5 ENTERPRISES INC.; TINOCO & ROGERS, LILIANA & BRANDON |
| `330` | 2 | JUAREZ, CRUZ & MARISOL ESCOBAR SANCHEZ; WARNKE & SARDINAS, KARINA & MARIO |
| `331` | 2 | GARFIAS, PAULINA; ZAMORA, JACOB R |
| `332` | 2 | MORALES & ITZEP, JUAN O; ZAMORA, CASSANDRA |
| `333` | 2 | ROJAS, LUIS & GUADALUPE MELENDREZ; SOLOMON, LAUREN |
| `571` | 2 | J DE LA OSA DENTAL INC; PRINCIC, KARL |
| `572` | 2 | PRINCIC, ALFREDA; SUPER DISCOUNT FAMILY STORE |
| `573` | 2 | MARISCAL, ROBERTO & VERONICA; SIGUENCIA, FERNANDO |
| `574` | 2 | ABOU ABDOU, AHMED & HIND ABED; ESCALANTE CASTRO, KARINA A |
| `575` | 2 | CORONA, BRIAN M; GAMBOA, RAFAEL MONARREZ |
| `576` | 2 | PALENCIA, VIVIAN A; RIVERA, OSCAR H JR |
| `577` | 2 | GUTIERREZ, FRANCISCO; SHARK TAND TRENDS INC |
| `578` | 2 | DAKAK, KALID K & SUMER; MORALES, GUSTAVO A |
| `579` | 2 | DAKAK, RAMI; GARCIA & CORONA, NICANOR & KRISTAL |
| `580` | 2 | NIETO AVENDANO, GIOVANNI & HEIN; NUNO, WILLIAM |
| `581` | 2 | GARCIA, MAGDALENA; PALMS GROVE LEASING LLC |
| `582` | 2 | GONZALEZ, ARACELY; SERRANO, JAVIER |
| `583` | 2 | GONZALEZ, EDUARDO; NESTOR, ELSY |
| `586` | 2 | GIRON, ESMERALDA; SOUTHBAY RESTORATION INC |
| `587` | 2 | PEREZ, ALYSSA M; SANTIAGO, CARMELA & ROY |
| `588` | 2 | RODDIS, JAMIE; SANTIAGO, EUNICE |
| `589` | 2 | DENISE, SANTIAGO; KIKB ENTERPRISES |
| `590` | 2 | NUNO, CYNTHIA; OSUA, IVAN & KARINA |
| `591` | 2 | NUNO, JUAN; VEGA, ERIN |
| `592` | 2 | CASTELLANOS, ROBERTO A & CELIA; GEORGE, DARON |
| `593` | 2 | ASCENCION, FATIMA B; SANCHEZ, AYLEEN |
| `594` | 2 | RUIZ GONZALEZ, MIGUEL A; SANCHEZ, BRYAN A |
| `595` | 2 | GIMENEZ  & GARCIA, ENRIQUE P & LUVIA; SANCHEZ, KEVIN IVAN |
| `596` | 2 | HUERTA, JACOB & STEPHANIE A; PEREZ MENDEZ, GENOVEVA |
| `597` | 2 | CREATE YOUR HEALTH LLC; SCHOEN, RAYMOND A |
| `598` | 2 | ROBLEDO & CORONA CORTES, PEDRO F & MARIA; ROJAS, ALEXANDER A |
| `599` | 2 | COVARRUBIAS & FERNANDEZ, MARIO & MONICA; ESCANDON, EDWARD M |
| `600` | 2 | KDEISS, RAYMOND E & LINDA R; MORENO, ISRAEL |
| `601` | 2 | GERRI CAN HELP INC; RAMIREZ HUERTA, ISMAEL |
| `602` | 2 | ENRIQUEZ REYES, MARCO & MARIA; FRANCO, CLAUDIA V |
| `603` | 2 | CARDENAS, MIGUEL; FRESH GARDEN FLOWERS, WHOLESALE INC |
| `604` | 2 | ANGUIANO R, JOSE R & ELSA R LANDEROS P; CABRERA, CAZARES |
| `605` | 2 | DIAZ LOPEZ, MARIA; GONZALEZ ROSAS, MELISSA |
| `606` | 2 | LAGUNA GARCIA, SERGIO A; SANCHEZ, CARLOS |
| `607` | 2 | MELENDEZ, YOLANDA; MOROYOQUI, JULIO |
| `608` | 2 | ARULLENDRAN, MAHENDRAN & DHANUSHA; MOROYOQUI, JULIO |
| `609` | 2 | CORONA, MARIO C & SARAH M; MOROYOQUI, JULIO |
| `610` | 2 | MOROYOQUI, JULIO; TAMASHIRO, KEVIN & BRENDA |
| `611` | 2 | CORONA, ZOYLA A; RIOS & CAZAR, TRINY & MARIA |
| `612` | 2 | SOBERANES, MAYA V; VILLEGAS, JEANNETTE & HECTOR |
| `613` | 2 | GARZA LOPEZ, LAURA & ARTURO; J&H MECHANICAL, INC |
| `614` | 2 | HERNANDEZ, ?; OLIVAREZ, RICHARD |
| `615` | 2 | M H GENERAL ENGINEERING INC; TRINIDAD, JEANETTE |
| `616` | 2 | GUERRERO, MICHAEL; RIZKALLAH, EMAD & MUNA |
| `617` | 2 | PMQ SOLUTIONS INC; SCHELSKE, DUSTIN N |
| `618` | 2 | BRENES RUIZ, JESUS & ROSSANNA; SCHELSKE, MICHAEL L & JOCELYNE |
| `619` | 2 | CORONA, MARCUS M; ZUNIGA, NETZAHUALCOYOTL & MARLLEN C GARC |
| `620` | 2 | LOPEZ VILLANUEVA, FRANCISCO; ROJAS, ALEX & ELVIA |
| `621` | 2 | DRE AND ASSOCIATES; FALATOONZADEH, HOSSEIN & SARA VAFAENIA |
| `622` | 2 | GALLEGOS, JOSE & NANCY BARRERA; M C ENGRAVING & JEWELRY INC |
| `624` | 2 | GONZALEZ, NATHALIE; THOME, SUSAN |
| `625` | 2 | ESPINOZA, MARIA; MORALES, KELLY A |
| `626` | 2 | GUTIERREZ, ASHLEY; SILVA SANCHEZ, JUAN CARLOS |
| `627` | 2 | DARWICH, NASSER M & JESSICA M; MELENDREZ BRAVO, GABRIELA |
| `628` | 2 | HUERTA MURO, JUAN C & MIRIAM L R; VALENZUELA, MARGARITA G |
| `630` | 2 | CALASANZ CHILDRENS FOUNDATION; OLIVA, CAREN J |
| `631` | 2 | HERREJON, NICHOLAS K; SUAREZ, RAQUEL E |
| `632` | 2 | ALL IN FUMIGATION; RODRIGUEZ, RAMIRO & BARBARITA |
| `633` | 2 | NUNO, ALEJANDRO; VELEZ, GERARDO & JAZMIN H |
| `634` | 2 | AN ADVISORS; SILVA, VINCENT & IRMA |
| `635` | 2 | ELSIE SILVA ESTATE; REVOLORIO, SALVADOR B |
| `636` | 2 | DARWICH, KATRINA F; VALENZUELA, ALEX & SANDRA |
| `637` | 2 | COVARRUBIAS II, JAVIER; DIAZ GUTIERREZ, ELIJAH B |
| `638` | 2 | COVARRUBIAS, DELIA; MARTINEZ, ARNULFO & SUSANNA |
| `639` | 2 | CUENCA, MARIA; NOPALTITLA, CINTIA |
| `640` | 2 | DURAN ROJAS, AGUSTIN & GUADALUPE; TRAN, MARTIN |
| `641` | 2 | CHEANG, SARAH; COLLAZO, MICHAEL |
| `642` | 2 | CHEANG, SARAH; GONZALEZ NAVARRO, REYES |
| `643` | 2 | CHEANG, LAURA; TULLIO, SHERRY |
| `644` | 2 | CHEANG, LAURA; GALINDO, NANCY D |
| `645` | 2 | ASFOUR, EMAD & RANDY K; CHEANG, VICTOR & NANCY |
| `646` | 2 | MARISCAL, MARIA E; MONTANO-ANDA, YVETTE |
| `647` | 2 | GALLARDO, LORENZO & MARTHA; TORREZ, AIDA A |
| `648` | 2 | JUAREZ, BEATRIZ M; SANCHEZ, JUAN & MELANIA |
| `649` | 2 | DEL TORO, ISAEL; HERNANDEZ, GERARDO HERNANDEZ |
| `650` | 2 | TAREEN, ILYAS; YANEZ RODRIGUEZ, LUIS M |
| `651` | 2 | LLAMAS, YADIRA; RAMIREZ, ESTEBAN C |
| `652` | 2 | LUM, ELLEN; YANEZ, ELVIRA |
| `653` | 2 | DURAN, AGUSTIN JR; YANEZ, ELVIA |
| `654` | 2 | KLEAN SOLAR SOLUTIONS, LLC; SALAZAR, ROSA |
| `655` | 2 | AGUIRRE, KAETE; SANDOVAL, GUSTAVO & SABRINA |
| `656` | 2 | LUM, ANTHONY & CHAYA; SANCHEZ, VALERIA |
| `657` | 2 | MALCOM, DAVID W & MARIANA C; RODDIS, LILIA |
| `658` | 2 | DAKAK, RAED K WIDA R; ESTATE OF DON A SEGESDY |
| `659` | 2 | DAKAK, JASMINE; MESSARRA, NOAH S |
| `660` | 2 | DAKAK, KALIL; GARCIA, JULIO D & ROXSANY Y |
| `661` | 2 | CALIFORNIA LIVEWIRE INC; GARCIA VEGA, JOSE |
| `662` | 2 | POLANCO, RICHARD; VEGA, JUAN |
| `663` | 2 | MUNOZ, JOSHUA; VOID |
| `664` | 2 | GONZALEZ, IVAN & GREISY; MUNOZ, JOSHUA |
| `665` | 2 | GAMBOA, SAUL; JOHNSON & LOPEZ JOHNSON, ALLEN S & CHRISTINE |
| `666` | 2 | CARDONA, PEDRO; REYNOSO, RICARDO & SANDY |
| `667` | 2 | LEDESMA, LILIANA; VILLALTA, CARLOS V |
| `668` | 2 | PORTALES, NORMAN JR; TORRES ARAIZA, LESLIE D |
| `669` | 2 | ARMENTA, LUISA; PORTALES, NORMAN & LIGIA |
| `671` | 2 | MARISCOS EL KORA DE NAYARIT; RIOS, OSCAR |
| `672` | 2 | RAMOS, MARITZA; RIOS, OSCAR |
| `673` | 2 | HERNANDEZ, GERARDO; NOPALTITLA, MIGUEL A & REYNA |
| `674` | 2 | HERNANDEZ, MAURICIO A; MERCADO, HECTOR |
| `675` | 2 | HUERTA, JOSE & CECILIA; VERGARA, JOSE MANUEL |
| `676` | 2 | MICLAT, ALEX JR & ELIZABETH; OROZCO, LUCIA |
| `677` | 2 | CAMACHO GARCIA, CLAUDIA L; MADRIGAL, ELADIO |
| `678` | 2 | JUAREZ, JOSE G; TASHAYOD, ALEX |
| `679` | 2 | GARCIA, LIONEL P; NOPALTITLA, MIGUEL A & REYNA |
| `680` | 2 | LAZO, BREANNA L; NOPALTITLA, MIGUEL A & REYNA |
| `681` | 2 | GARCIA-HUBLER, SANDRA; NOPALTITLA, MIGUEL A & REYNA |
| `682` | 2 | MICLAT, BRIANA; O'CAMPO, MARIA |
| `683` | 2 | MICLAT III, ALEX M; O'CAMPO, MARIA |
| `684` | 2 | MURILLO, MARISA M; OCHOA, SERGIO & MARTHA |
| `685` | 2 | ALVARADO, OSCAR & DESIREE; FIBER ERA INC |
| `686` | 2 | ARANDA, HUGO & GEORGINA; CARDENAS NAVARRO, MARIA DEL CARMEN |
| `687` | 2 | APK PLUMBING INC; VELASQUEZ, BRYAN O |
| `714` | 2 | HERNANDEZ, ISMAEL; MAR FIBER COMMUNICATIONS, LLC |
| `715` | 2 | ANGULO CASTANEDA, JOSE E; PEREZ, TERRY |
| `716` | 2 | LOPEZ, RAFAEL; VELASQUEZ, MAYNOR |
| `717` | 2 | JIMENEZ, LETICIA C; ORTIZ, ISRAEL |
| `721` | 2 | LAZO, ANNABEL; SANCHEZ HERNANDEZ, ADRIAN & ADRIANA |
| `722` | 2 | HERNANDEZ CUELLAR, SALVADOR & NORA; MENDEZ, SERGIO |
| `723` | 2 | HERNANDEZ, ADELAIDA; LEMUS, EVELYN C |
| `724` | 2 | EDWARD LOPEZ AMERICAN DENTAL INC; HERNANDEZ, SERGIO |
| `725` | 2 | BETANCOURT, RUDY; HERNANDEZ, SALVADOR G |
| `726` | 2 | LOPEZ, WILFREDO & MERCEDES; MANDIN, EDMOND A |
| `727` | 2 | REYES SOLIS, GASPAR & NORMA; TINOCO, JOSE & ESMERALDA |
| `728` | 2 | GUTIERREZ, IVAN & DEANA; MARQUEZ, JOANNA |
| `729` | 2 | CAMACHO, ADRIAN & KARINA; MARQUEZ, RICARDO & NORMA |
| `730` | 2 | BRENES, REYNA E; PEREZ, EDMUNDO O & HONORINA |

### Family same-surname repeats (sample)

**2** keys. First 25:

| Bare | n_rows | Names |
|---|---:|---|
| `623` | 2 | GONZALEZ, GLORIA; GONZALEZ, PEDRO & MARIA |
| `720` | 2 | GARCIA SERNAS, ANGELICA M; GARCIA, JAVIER & ANA R NUNO FRANCO |

### Legitimate same-person repeats (count only)

**0** bare logs are multi-row with one normalized person — not collisions for L0 purposes.

## Acceptance vs R0 Wave 1

| Criterion | Status |
|---|---|
| Zero Drake bare-log collisions on re-export | **BLOCKED** — office must edit Drake/Log, then re-export |
| Log-internal repeats classified | **DONE** — see above |
| A4 I7 PASS | **BLOCKED** on bare `141` until collision cleared |

## Next actions (human)

1. Fix bare `141` first (I7 + three claimants).
2. Work `b_genuine_reuse` Drake queue (assign new logs).
3. Decide household rule, then clear `c_family*`.
4. Clear malformed buckets (truncated / prior-season / out-of-range).
5. Split Tax Log `genuine_reuse` keys before any Log→TaxOps write-back design.
6. Re-export `TAXPAYER.csv` → A0→A4.

Machine-readable: `T:\audit\investigation\W1-log-integrity.json`
