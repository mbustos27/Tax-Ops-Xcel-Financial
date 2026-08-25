# W4 — cross-client spouse contamination

_Generated: 2026-08-11T23:56:42Z · **read-only** · no TaxOps writes_

## Verdict

Two distinct defect classes:

1. **Drake-import mis-attachment** — `spouses.taxpayer_name` names a *different* household than `spouses.client_id`. This is how MARTINEZ/QUINTANA/MUNGUIA got someone else's spouse. Source = `TY2025 Drake import`, **not** the Wave 4 fold.
2. **Fold propagation** — `wave4_clients_fold` copied pre-existing `clients.spouse_*` values; fold personnel rate is low vs Drake-import.

| Universe | Rows | Personnel (wrong person) | Rate |
|---|---:|---:|---:|
| All `spouses` | 520 | 144 | 27.7% |
| `wave4_clients_fold` | 354 | 18 | 5.1% |
| Drake import (personnel subset) | — | 126 | — |
| `taxpayer_name` disjoint from owner | — | 114 | — |
| `clients.spouse_*` filled | 406 | 28 | 6.9% |

Open `SPOUSE_STORE_DIVERGENCE` overlapping personnel contam: **14** / 163.

## W5 known examples (confirm class)

| Owner | Spouse on record | Source | taxpayer_name | Signal |
|---|---|---|---|---|
| MARTINEZ, ARNULFO & SUZANNA A (`610`, last4 `5949`) | TAREEN, FOUZIA | `TY2025 Drake import` | ZULQARNIAN & FOUZIA TAREEN | **taxpayer_name_mismatch** (Drake mis-attach) |
| QUINTANA JR, ROGELIO & KAYLA M QUINTANA (`819`, last4 `5180`) | NELSON, PATRICIA | `TY2025 Drake import` | KURT & PATRICIA NELSON | **taxpayer_name_mismatch** (Drake mis-attach) |
| MUNGUIA, FEDERICO & MARIBELMUNOZ (`145`, last4 `3527`) | RUIZ DE PEREZ, HONORINA | `TY2025 Drake import` | EDMUNDO PEREZ & HONORINA RUIZ DE PEREZ | **taxpayer_name_mismatch** (Drake mis-attach) |

Note: `clients.spouse_*` on MARTINEZ already has SUZANNA (correct). The wrong person lives on the `spouses` Drake-import row. Fold did not create these three.

## All personnel hits (144)

| Owner id | Owner | Spouse | Source | Reasons |
|---:|---|---|---|---|
| 50 | ALVAREZ, RUBI | BRAVO, ELISA | `TY2025 Drake import` |  |
| 72 | ARMENTA, LUISA D | VALENCIA, DEBORAH | `TY2025 Drake import` |  |
| 104 | BETANCOURT, RUDY | SUKKAR, LINA | `TY2025 Drake import` |  |
| 113 | BRAVO, VICTORIA | GARCIA VILLAREAL, IRAZU | `TY2025 Drake import` |  |
| 119 | BURGOIN, LOUIE & FRANCES A | BURGOIN, FRANCES A | `TY2025 Drake import` |  |
| 137 | CAMERO, EDNA | , IMELDA | `TY2025 Drake import` |  |
| 145 | MUNGUIA, FEDERICO & MARIBELMUNOZ | RUIZ DE PEREZ, HONORINA | `TY2025 Drake import` |  |
| 167 | CEBALLOS, DANIEL | TAHANI, MARIA | `TY2025 Drake import` |  |
| 171 | CERVANTES, VICTOR M | CERVANTES, CHRISTINA | `TY2025 Drake import` |  |
| 178 | CHAVEZ, SANDRA | ESPINOZA, SANDY | `TY2025 Drake import` |  |
| 179 | CONTRERAS & MAYORGA, DANIEL & CRISTINA | MAYORGA, CRISTINA | `TY2025 Drake import` |  |
| 183 | CHOWDHURY, TANVIR & SUKRA B | MORENO, MARIA | `TY2025 Drake import` |  |
| 222 | DAKAK, JASMINE | DAKAK, WIDAD | `TY2025 Drake import` |  |
| 227 | DARWICH, KATRINA F | DARWICH, JESSICA | `TY2025 Drake import` |  |
| 235 | DEL TORO, ISAEL | CHEVEZ, MARY | `TY2025 Drake import` |  |
| 240 | DELGADO, NORMA A | CERVANTES, JUAN | `TY2025 Drake import` |  |
| 247 | DIAZ, FRANCISCO J & ROSA M | ORTEGA, CLAUDIA L | `TY2025 Drake import` |  |
| 251 | DINA TRANSPORT INC, None | CASTILLO, MARTHA | `TY2025 Drake import` |  |
| 291 | EXPERIMENTAL SCREEN PRINTING INC, None | SARDINAS, MARIO | `TY2025 Drake import` |  |
| 294 | FALATOONZADEH, HOSSEIN & SARA VAFAEENIA | MELENDREZ, M GUADALUPE | `TY2025 Drake import` |  |
| 296 | FAVELA, RUTH | MELENDREZ DE GALLEGO, M | `TY2025 Drake import` |  |
| 297 | FAVELA, SAUL | ROSAS, KARLA | `TY2025 Drake import` |  |
| 322 | GALARZA, AURORA | GARCIA AVALOS, NORMA | `TY2025 Drake import` |  |
| 343 | GARCIA, JASMINE | LUNA, JESSICA | `TY2025 Drake import` |  |
| 350 | GARCIA, MARIA E | GALLARZO, LIDUVINA | `TY2025 Drake import` |  |
| 373 | GOMEZ BONILLA, JOSELYN M | , ALMA | `TY2025 Drake import` |  |
| 389 | GONZALEZ, ARACELY | MOJARRO, MARIA  D J | `TY2025 Drake import` |  |
| 393 | GONZALEZ, FITZGERALD L & VERONICA C | YANEZ, MARIA | `TY2025 Drake import` |  |
| 396 | GONZALEZ, JESUS C & CLAUDIA A | AGUILA, MIGUEL | `TY2025 Drake import` |  |
| 397 | GONZALEZ, JESUS R & MARIA | CASTANEDA, VICTOR | `TY2025 Drake import` |  |
| 400 | AMIN & KHOKHAR, MOHAMMAD & NOSHEEN | KHOKHAR, NOSHEEN | `TY2025 Drake import` |  |
| 411 | GUERRERO ESTRADA, JEANINE | RODAS, MARTHA | `TY2025 Drake import` |  |
| 435 | HAMIDA, WALID & DANA K | , SAEKO | `TY2025 Drake import` |  |
| 436 | HAMIDEH, AMJAD G | RAMIREZ STETAR, JOSE | `TY2025 Drake import` |  |
| 438 | HAMIDEH, LAILA N & AKRAM | , SUZANNA | `TY2025 Drake import` |  |
| 451 | HERNANDEZ GARCIA, ALEXANDER | GONZALES, HASSIE | `TY2025 Drake import` |  |
| 459 | HERNANDEZ, GERARDO | NUNO FRANCO, ANA ROSA | `TY2025 Drake import` |  |
| 464 | HERNANDEZ, MAURICIO A | JAIME, CLAUDIA | `TY2025 Drake import` |  |
| 468 | HERNANDEZ, NICOLE | ALFONSO DE CARRANZA, KATERIN | `TY2025 Drake import` |  |
| 475 | HESSE, ERIC R & MARIA J | , MANUEL | `TY2025 Drake import` |  |
| 477 | ZAMORA, CASSANDRA | ZAMORA, JACOB | `TY2025 Drake import` |  |
| 487 | INDUSTRIAL MACHINERY AUTOMATION SVC, None | KDEISS, THERESE | `TY2025 Drake import` |  |
| 493 | J MEJIA TRUCKING INC, None | PINO, MAYRA | `TY2025 Drake import` |  |
| 495 | HUERTA, DANNY & BRIDGETTE | HUERTA, EVELYN | `TY2025 Drake import` |  |
| 497 | JIMENEZ, DAMIAN A | , SUKRA | `TY2025 Drake import` |  |
| 511 | JULIO, JOSE | RODRIGUEZ PENA, BARBARITA | `TY2025 Drake import` |  |
| 522 | LAGUNA, SERGIO | CORONA, KRISTAL | `TY2025 Drake import` |  |
| 535 | LEDESMA, DAVID D & ALICIA D | , ROY | `TY2025 Drake import` |  |
| 555 | LLAMAS, YADIRA | , BLANCA | `TY2025 Drake import` |  |
| 557 | LOMELI, SOCORRO | , DHANUSHA | `TY2025 Drake import` |  |
| 559 | LOPEZ VILLANUEVA, FRANCISCO M | TAMASHIRO, BRENDA | `TY2025 Drake import` |  |
| 562 | LOPEZ, DAVID | , MARIA | `TY2025 Drake import` |  |
| 564 | LOPEZ, PAOLA R | , VANESSA | `TY2025 Drake import` |  |
| 570 | LOYA, ELIZABETH | ROJAS, JOSE | `TY2025 Drake import` |  |
| 572 | LUM, ELLEN | VAFAEENIA, SARA | `TY2025 Drake import` |  |
| 602 | MARQUEZ, ADRIAN | ASFOUR, RANDA | `TY2025 Drake import` |  |
| 610 | MARTINEZ, ARNULFO & SUZANNA A | TAREEN, FOUZIA | `TY2025 Drake import` |  |
| 629 | MEDINA, SYLVIA | MUNOZ, MARIBEL | `TY2025 Drake import` |  |
| 634 | MEJIA JULIO, JUAN | , XOCHIL | `TY2025 Drake import` |  |
| 654 | MIAMAR FUTURE LLC, None | , REBECCA | `TY2025 Drake import` |  |
| 658 | MICLAT, BRIANA | VELASQUEZ, MARIA E | `TY2025 Drake import` |  |
| 666 | MOLINA, VICTOR M | HERNANDEZ C, ADRIANA | `TY2025 Drake import` |  |
| 683 | MORALES, IRMA | , RAQUEL | `TY2025 Drake import` |  |
| 698 | MORENO, WILLIAM J & MARIA D | LANDEROS P, ELSA | `TY2025 Drake import` |  |
| 719 | NESTOR, CRYSTAL | CALZADILLA, STEPHANIE | `TY2025 Drake import` |  |
| 726 | NUNEZ, DANIEL E & GLORIA MARIN | CAMPOS, NORMA | `TY2025 Drake import` |  |
| 730 | NUNO, JESSICA A | , CLAUDIA | `TY2025 Drake import` |  |
| 731 | NUNO, JUAN | GUERRERO QUIROZ, JOSE | `TY2025 Drake import` |  |
| 738 | OBREGON, DENNIS | RIZKALLAH, MUNA H | `TY2025 Drake import` |  |
| 751 | OLIDEN, ANDRES & STEPHANIE P VALENCIA | , DIANA | `TY2025 Drake import` |  |
| 754 | ORMA SERVICES INC, None | DIAZ, ROSA | `TY2025 Drake import` |  |
| 760 | ORTIZ, ISRAEL | , GREGORIA | `TY2025 Drake import` |  |
| 779 | PALENCIA, VIVIAN A | , GUADALUPE | `TY2025 Drake import` |  |
| 787 | PENA, SANDRA | WASHINGTON, WILLIE | `TY2025 Drake import` |  |
| 796 | PEREZ, MIA A | LOPEZ-MURILLO, LORETTA | `TY2025 Drake import` |  |
| 808 | VELEZ, GERARDO & JAZMIN H | VELEZ, JOSEFINA | `TY2025 Drake import` |  |
| 819 | QUINTANA JR, ROGELIO & KAYLA M QUINTANA | NELSON, PATRICIA | `TY2025 Drake import` |  |
| 821 | QUINTANA, JULIO | EL-SAID, NAHLA | `TY2025 Drake import` |  |
| 824 | QUINTERO, CECILIA | , NORMA | `TY2025 Drake import` |  |
| 827 | RAMIREZ, ELIZABETH | CHEANG, NANCY | `TY2025 Drake import` |  |
| 857 | REYES AGUILAR, GENESIS N | , PAMELA | `TY2025 Drake import` |  |
| 866 | SERRANO VELIS, MARICELA & KEVIN M | , MARIA | `TY2025 Drake import` |  |
| 868 | RIOS, TRINY & COLLETE M CAZAR | VELIS, KEVIN | `TY2025 Drake import` |  |
| 878 | RODAS, GUSTAVO & CARIDAD | HESSE, MARIA J | `TY2025 Drake import` |  |
| 880 | PORTALES, NORMAN & LIGIA | , MARIA | `TY2025 Drake import` |  |
| 881 | RODDIS, LILIA | PORTALES, LIGIA | `TY2025 Drake import` |  |
| 884 | RODRIGUEZ, ARIEL N | LOPEZ, ROSALVA | `TY2025 Drake import` |  |
| 887 | RODRIGUEZ, GABRIEL T & AMELIA E | , AKRAM | `TY2025 Drake import` |  |
| 899 | JIMENEZ, ALBERT J | JIMENEZ, ALICIA | `TY2025 Drake import` |  |
| 904 | ROMERO, GRISELDA | MONTANEZ, JOANNA | `TY2025 Drake import` |  |
| 905 | ROMERO, JAY & MARTHA R | TORRES, T | `TY2025 Drake import` |  |
| 907 | ROSALES, JUAN M & MARIA | CASTILLO, HAIDE | `TY2025 Drake import` |  |
| 909 | ROSAS, RICARDO & BRAVO GARCIA, MARTHA | KAVANAGH, KIMARA | `TY2025 Drake import` |  |
| 918 | SALAS DIAZ, CESAR L | GAMBOA, MARTHA | `TY2025 Drake import` |  |
| 927 | SANCHEZ HERNANDEZ, MIRIAM S | BARELA, MARY E | `TY2025 Drake import` |  |
| 930 | SANCHEZ, BRYAN | , DELIA | `TY2025 Drake import` |  |
| 932 | SANCHEZ, DANIEL & LORENA | , GLORIA | `TY2025 Drake import` |  |
| 940 | SANDOVAL, ADRIANA | SANDOVAL, SABRINA | `TY2025 Drake import` |  |
| 942 | SANDOVAL, GABRIEL | ZUNIGA NAVARRO, SUSANA | `TY2025 Drake import` |  |
| 944 | SANDOVAL, GUSTAVO & SABRINA | , LUCY | `TY2025 Drake import` |  |
| 948 | SANTIAGO, EUNICE | CACERES, CLAUDIA | `TY2025 Drake import` |  |
| 950 | SAUCEDO, HECTOR J | , DANA | `TY2025 Drake import` |  |
| 954 | ROSALES, JUAN & MARIA | ROSALES, MARIA | `TY2025 Drake import` |  |
| 980 | SOTO LOPEZ, MATILDE | , BEATRIZ | `TY2025 Drake import` |  |
| 981 | SOTO, RICHARD | MEDRANO, NUBIA | `TY2025 Drake import` |  |
| 993 | TAMASHIRO, KEVIN & BRENDA | PINEDA, MARGARITA D | `TY2025 Drake import` |  |
| 1014 | TORRES, ALEJO V & ROSA M | DURAN-VASQUEZ, PALOMA | `TY2025 Drake import` |  |
| 1023 | TRUJILLO, FRANCISCO | , DORA | `TY2025 Drake import` |  |
| 1042 | VARGAS, CELESTINO L | GALLARDO, MARTHA | `TY2025 Drake import` |  |
| 1066 | VELEZ, SALVADOR G & ZONIA | , CHRISTINA | `TY2025 Drake import` |  |
| 1082 | VILLAR HERNANDEZ, ROBERTO | MARROQUIN, DESTINY | `TY2025 Drake import` |  |
| 1083 | VILLAREAL, RICHARD P | , LINDA | `TY2025 Drake import` |  |
| 1085 | VILLEGAS, JEANNETTE E & HECTOR A | MERAZ, FLORENTINA | `TY2025 Drake import` |  |
| 1107 | YANEZ, LUCILA | , DAINER | `TY2025 Drake import` |  |
| 1110 | YBARRA, ELIZABETH | CASTILLO, M | `TY2025 Drake import` |  |
| 1115 | ZIRKELBACH, MAX E & SAEKO | , MARTHA | `TY2025 Drake import` |  |
| 1117 | ZUNIGA, QUETZALCOATL | LINARES, ISABEL | `TY2025 Drake import` |  |
| 1119 | PADILLA LUCIO, JOSE | VILLAPUDUA, BRIDGET | `TY2025 Drake import` |  |
| 1273 | SALAMA, ENGIE | GHIANI, KEVIN | `TY2025 Drake import` |  |
| 1338 | POLANCO, RICHARD | , JOCELYNE | `TY2025 Drake import` |  |
| 1355 | BARELA, ALBERT H | LAGUNA, CARMEN | `TY2025 Drake import` |  |
| 1382 | MORENO, ISRAEL | HUNT, MARY | `TY2025 Drake import` |  |
| 1483 | TRAN, MARTIN | , YURIKO | `TY2025 Drake import` |  |
| 1502 | MAZARIEGOS HERNANDEZ, MARILANDA | SERRANO, ELVIRA | `TY2025 Drake import` |  |
| 1511 | BRICENO, JUAN | CHABOLLA M, TERESA | `TY2025 Drake import` |  |
| 1540 | SALAMA, AYMAN | OCHOA, ESPERANZA | `TY2025 Drake import` |  |
| 38 | AJRAB, YOUSEF R & KDEISS, NATALIE | M KDEISS, NATALIE | `wave4_clients_fold` |  |
| 51 | AMIN, MOHAMMAD J & NOSHEEN KHOKHAR | KHOKHAR, NOSHEEN | `wave4_clients_fold` |  |
| 105 | BLANCAS, JOSE & MARIA DEL CARMEN | DEL CARMEN, MARIA | `wave4_clients_fold` |  |
| 195 | CONTRERAS, DANIEL & CRISTINA MAYORGA | CRISTINA, MAYORGA | `wave4_clients_fold` |  |
| 353 | GARCIA, NICANOR & KRISTAL CORONA | CORONA, KRISTAL | `wave4_clients_fold` |  |
| 403 | GONZALEZ, PEDRO & MARIA | GONZALEZ, MARIA | `wave4_clients_fold` |  |
| 534 | LEDESMA, BLANCA E & ADRIAN | LEDESMA, ADRIAN | `wave4_clients_fold` |  |
| 563 | GARZA LOPEZ, LAURA & ARTURO | LOPEZ, ARTURO | `wave4_clients_fold` |  |
| 875 | RODARTE, ANASTACIO N & NATALIA | RODARTE, NATALIA | `wave4_clients_fold` |  |
| 1303 | VASQUEZ, ROBERT | VASQUEZ PALOMA, DURAN | `wave4_clients_fold` |  |
| 1444 | VASQUEZ, RAMON | PINEDA, LIZBETH | `wave4_clients_fold` |  |
| 1459 | MICLAT JR, ALEX | MICLAT, ELIZABETH | `wave4_clients_fold` |  |
| 1469 | GIMENEZ, ENRIQUE | M GARCIA, LUVIA | `wave4_clients_fold` |  |
| 1670 | DURAN, AGUSTIN | DURAN, GUADALUPE | `wave4_clients_fold` |  |
| 2289 | SOLIS, JESUS | SOLIS, SONIA | `wave4_clients_fold` |  |
| 2301 | SILVA, VINCENT J | SILVA, IRMA | `wave4_clients_fold` |  |
| 2370 | GARCIA, JOSE I | D HERNANDEZ, MARIA | `wave4_clients_fold` |  |
| 2399 | ARROYO, HUMBERTO | MARIA, HERNANDEZ | `wave4_clients_fold` |  |

Machine: `T:\audit\investigation\W4-spouse-contamination.json`
