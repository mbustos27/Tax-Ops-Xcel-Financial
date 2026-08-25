# W1 — genuine_reuse spurious co-occupants (solo-bare test)

_Generated: 2026-08-12T23:14:11Z · read-only_

Rule: if a name on a `genuine_reuse` bare also appears as the **sole** name on any other bare, its repeat-bare row is spurious — drop it. When one keeper remains, the bare resolves.

| Metric | n |
|---|---:|
| genuine_reuse bares | 204 |
| of which 2-person pairs | 200 |
| `RESOLVE_SINGLE` | 19 |
| `ALL_SPURIOUS` | 1 |
| `STILL_COLLISION` | 184 |

## Resolves to single holder (19)

| Bare | Keep | Drop (solo home) |
|---|---|---|
| `1` | CEST LA VIE APPAERL INC | AZUSA HOMES LLC -> `90` |
| `2` | BRAVO, BYRON M | LIONS FUMIGATION INC -> `63`,`1024` |
| `9` | LECHUGA, REBECCA | CATALPA DEVELOPMENT LLC -> `93` |
| `146` | BURGOIN, LOUIE & FRANCES A | SANDOVAL AUTO SERVICE & TOW -> `131`,`129`,`130`,`128` |
| `147` | SANTANA, HERAS | SANDOVAL, RUDY & CLAIRE -> `124`,`126`,`125`,`127` |
| `148` | TAREEN, ZULQARNIAN & FOUZIA | LOS BOMBEROS DE LA COUNTY -> `36` |
| `273` | CHEVEZ, LEONEL & MARY | SALAZAR QUIROZ, ROSA M -> `293` |
| `279` | SERRANO, JORGE | RAMIREZ, ELIZABETH -> `758` |
| `282` | LEDESMA, BLANCA & ADRIAN | SANCHEZ, JORGE -> `477` |
| `292` | MORENO, WILLIAM & MARIA | MELENDEZ CORVERA, ELVIS M -> `364` |
| `605` | GONZALEZ ROSAS, MELISSA | DIAZ LOPEZ, MARIA -> `97` |
| `643` | TULLIO, SHERRY | CHEANG, LAURA -> `856` |
| `644` | GALINDO, NANCY D | CHEANG, LAURA -> `856` |
| `645` | ASFOUR, EMAD & RANDY K | CHEANG, VICTOR & NANCY -> `857` |
| `646` | MONTANO-ANDA, YVETTE | MARISCAL, MARIA E -> `861`,`860` |
| `663` | MUNOZ, JOSHUA | VOID -> `1121` |
| `666` | REYNOSO, RICARDO & SANDY | CARDONA, PEDRO -> `887` |
| `671` | RIOS, OSCAR | MARISCOS EL KORA DE NAYARIT -> `894` |
| `722` | HERNANDEZ CUELLAR, SALVADOR & NORA | MENDEZ, SERGIO -> `829` |

## All occupants have solo homes (1)

Every name on the bare also owns a non-repeat bare — bare has no residual keeper under this rule (office: pick canonical or clear).

| Bare | Occupants → solo homes |
|---|---|
| `4` | MACKAY, JEANNE -> `249`; ORMA SERVICES, INC -> `12`; PEREZ, NORA L -> `1117` |

## Still collision (184)

- Partial drop (spurious removed but ≥2 keepers left): **2**
- No solo home for any occupant: **182**

### Partial

| Bare | Keepers left | Dropped |
|---|---|---|
| `6` | ALDAMA, MAYRA, SOTELO, MICHELLE | ORMA SERVICES, INC |
| `8` | ALVARADO, ERNESTO G & REBECCA, PARTIDA, DYMENIQUE | ORMA SERVICES  INC |

### No solo home (sample of 25/182)

| Bare | Names |
|---|---|
| `3` | BOCANEGRA GALLEGOS, URIEL & ADRIANA; CHEANG, SARAH |
| `5` | CASILLAS, MARIA DEL CARMEN; PADILLA, CRISTIAN F |
| `7` | CASILLAS, MARIA DEL CARMEN; GARCIA PARTIDA, JACQUELINE |
| `141` | CHAN, THY; PEREZ & GARCIA VILLAREAL, ANDRES & IRAZU |
| `142` | AGUILAR & GARCIA ENTERPRISES; L.A. AUTO SERVICE LLC |
| `143` | LA MESA AUTO SALES INC.; VILLAREAL, RICHARD P |
| `144` | RECENDEZ, FRANCISCO D & DAISY CASAS; SALAS, SERGIO D |
| `145` | BEDARD, PETER A; GALICIA MELCHOR, GISELA |
| `149` | DORIA, RUDY J DORIA; LOPEZ, JUAN |
| `150` | SANDOVAL, GERARDO V; VIDAL, LUIS M |
| `151` | GARCIA, FRANCISCO; MULTICARD SYSTEMS |
| `152` | CARRANZA, MAURICIO A; PACIFIC TRUCK |
| `181` | ABU TAHA, QAIS AHMAD YOUNIS; VELASQUEZ, CARLOS & MARIA E |
| `182` | CORNWELL, JOHN; GUERRERO, VICTOR M |
| `183` | CORNWELL, JOHN; JUAREZ, EDWVIGES |
| `184` | CORNWELL, JOHN; JUAREZ, EDWVIGES |
| `185` | CORNWELL, JOHN; MACIAS, DENNIS N |
| `186` | CAZARES, MARIA E; TRIGEROUS, NOELLE M |
| `271` | GONZALEZ, SHAWN M; VARGAS, CELESTINO L |
| `272` | GONZALEZ, AURELIO R; ROMERO, GRISELDA |
| `274` | PEREZ & GONZALEZ, MICHAEL J & CRYSTAL; YOUSEF, QASEM |
| `275` | FLORES, CIPRIANO & MARIA; THE PRINCESS MATTRESS INC |
| `276` | AGUIRRE JIMENEZ, AMALIA; HUERTA, JUAN & ALICIA |
| `277` | CRUZ, GEORGE L; SERRANOS TIRES & AUTO REPAIRS CORP |
| `278` | MONARREZ & CERVANTES, BEATRIZ & JUAN; SERRANO LOPEZ, JESUS & JOVITA |

Machine: `T:\audit\investigation\W1-reuse-spurious-solo.json`
